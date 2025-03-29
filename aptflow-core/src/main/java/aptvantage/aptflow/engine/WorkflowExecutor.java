package aptvantage.aptflow.engine;

import aptvantage.aptflow.api.RunnableWorkflow;
import aptvantage.aptflow.engine.persistence.WorkflowRepository;
import aptvantage.aptflow.model.WorkflowRun;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.google.common.flogger.FluentLogger;
import org.awaitility.Awaitility;

import javax.sql.DataSource;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class WorkflowExecutor {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final ThreadLocal<ExecutionContext> executionContext = new ThreadLocal<>();

    private final Scheduler scheduler;
    private final WorkflowRepository workflowRepository;

    private final Set<Object> workflowDependencies;

    private final CompleteSleepTask completeSleepTask;
    private final ResumeStartedWorkflowTask resumeStartedWorkflowTask;
    private final SignalWorkflowTask signalWorkflowTask;
    private final StartWorkflowTask startWorkflowTask;

    public WorkflowExecutor(DataSource dataSource, WorkflowRepository workflowRepository, Set<Object> workflowDependencies) {
        this.completeSleepTask = new CompleteSleepTask(workflowRepository, this);
        this.resumeStartedWorkflowTask = new ResumeStartedWorkflowTask(this);
        this.startWorkflowTask = new StartWorkflowTask(workflowRepository, this);
        this.signalWorkflowTask = new SignalWorkflowTask(workflowRepository, this);
        this.scheduler = Scheduler
                .create(dataSource,
                        startWorkflowTask,
                        completeSleepTask,
                        signalWorkflowTask,
                        resumeStartedWorkflowTask)
                .pollingInterval(Duration.ofSeconds(1))
                .enableImmediateExecution()
                .build();
        this.workflowRepository = workflowRepository;
        this.workflowDependencies = workflowDependencies;
    }

    /**
     * Class must have only one public constructor or have a single public constructor annotated with @Inject
     *
     * @param clazz
     * @return
     */
    private static Constructor findInjectableConstructor(Class clazz) {
        List<Constructor> list = Arrays.stream(clazz.getConstructors())
                .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
                .toList();
        if (list.size() == 1) {
            return list.get(0);
        }
        throw new UnsupportedOperationException("still need to handle multiple constructors with @Inject");
    }

    public void start() {
        this.scheduler.start();
    }

    public void stop() {
        this.scheduler.stop();
    }

    public ExecutionContext getExecutionContext() {
        ExecutionContext ctx = executionContext.get();
        if (ctx == null) {
            throw new IllegalStateException("No execution context available. This method cannot be called outside of a RunnableWorkflow.execute call stack");
        }
        return ctx;
    }

    void executeWorkflow(String workflowId) {
        WorkflowRun<Serializable, Serializable> workflowRun = this.workflowRepository.getWorkflowRun(workflowId);
        try {
            executionContext.set(new ExecutionContext(workflowId));
            RunnableWorkflow instance = instantiate(workflowRun.workflow().className());
            Serializable output = instance.execute(workflowRun.workflow().input());
            this.workflowRepository.workflowRunCompleted(workflowId, output);
            logger.atInfo().log("Workflow [%s] is complete", workflowId);
        } catch (AwaitingSignalException e) {
            logger.atInfo().log("Pausing execution of workflow [%s] to wait for signal [%s]", workflowId, e.getSignal());
        } catch (WorkflowStillSleepingException e) {
            logger.atInfo().log("Workflow [%s] has been sleeping [%s] for [%s] out of [%s]", workflowId, e.getIdentifier(), e.getElapsedSleepTime(), e.getNapTime());
        } catch (WorkflowSleepingException e) {
            logger.atInfo().log("Pausing execution of workflow [%s] to sleep [%s] for [%s]", workflowId, e.getIdentifier(), e.getNapTime());
        } catch (ConditionNotSatisfiedException e) {
            logger.atInfo().log("Pausing execution of workflow [%s] because condition [%s] is not satisfied", workflowId, e.getIdentifier());
        } catch (Exception e) {
            // TODO -- save some kind of Failure data with the failed workflow
            logger.atSevere().withCause(e).log("Workflow [%s] execution failed", workflowId);
            this.workflowRepository.failWorkflowRun(workflowId);
        } finally {
            executionContext.remove();
        }
    }

    private RunnableWorkflow instantiate(String className) {
        try {
            Class workflowClass = Class.forName(className);
            Constructor constructor = findInjectableConstructor(workflowClass);

            Object[] args = Arrays.stream(constructor.getParameterTypes())
                    .map(type -> workflowDependencies.stream()
                            .filter(o -> type.isAssignableFrom(o.getClass()))
                            .findFirst()
                            .orElseThrow(() -> new NoSuchElementException("Could not find a constructor arg match for type [%s] on class [%s]".formatted(type, workflowClass))))
                    .toArray();
            return (RunnableWorkflow) constructor.newInstance(args);

        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void scheduleReevaluation(String workflowId, String conditionId, Instant resumptionTime) {
        RunWorkflowTaskInput input = new RunWorkflowTaskInput(workflowId);
        String taskInstanceId = "%s::%s::%s".formatted(workflowId, conditionId, UUID.randomUUID().toString());
        scheduler.schedule(this.resumeStartedWorkflowTask.instance(
                taskInstanceId, input), resumptionTime);
    }

    public void scheduleWakeUp(String workflowId, String sleepId, Instant wakeupTime) {
        CompleteSleepTaskInput input = new CompleteSleepTaskInput(workflowId, sleepId);
        scheduler.schedule(this.completeSleepTask.instance(
                "sleep::%s::%s".formatted(workflowId, sleepId), input
        ), wakeupTime);
    }

    public <T extends Serializable> void signalWorkflowRun(String workflowId, String signalName, T signalValue) {
        logger.atInfo().log("received signal [%s::%s]", workflowId, signalName);
        //TODO -- validate the signalValue is of the expected type
        TaskInstance<SignalWorkflowTaskInput> instance = signalWorkflowTask.instance(
                "signal::%s::%s".formatted(workflowId, signalName),
                new SignalWorkflowTaskInput(workflowId, signalName, signalValue));
        scheduler.schedule(instance, Instant.now());
        //TODO - track signal sent in addition to signal received
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> this.workflowRepository.isSignalReceived(workflowId, signalName));
    }

    public <I extends Serializable, O extends Serializable> void runWorkflow(Class<? extends RunnableWorkflow<I, O>> workflowClass, I workflowParam, String workflowId) {
        logger.atInfo().log("scheduling run for new workflow [%s] of type [%s]", workflowId, workflowClass.getName());
        String workflowRunId = this.workflowRepository.scheduleRunForNewWorkflow(workflowId, workflowClass, workflowParam);
        startRun(workflowId, workflowRunId);
    }

    public void reRunWorkflowFromStart(String workflowId) {
        // TODO - test this should fail if workflow does not exist
        // TODO - test this should fail if existing workflow's latest run is not in a terminal state
        logger.atInfo().log("scheduling re-run of existing workflow [%s]", workflowId);
        String workflowRunId = workflowRepository.scheduleNewRunForExistingWorkflow(workflowId);
        startRun(workflowId, workflowRunId);
    }

    private void startRun(String workflowId, String workflowRunId) {
        TaskInstance<RunWorkflowTaskInput> instance = startWorkflowTask.instance(
                "workflow::%s::%s".formatted(workflowId, workflowRunId),
                new RunWorkflowTaskInput(workflowRunId));
        scheduler.schedule(instance,
                Instant.now());
    }

    public <R extends Serializable> CompletableFuture<R> supplyAsync(Supplier<R> supplier) {
        ExecutionContext ctx = this.getExecutionContext();
        return CompletableFuture.supplyAsync(() -> {
            this.executionContext.set(ctx);
            R r = supplier.get();
            this.executionContext.remove();
            return r;
        });
    }

    public CompletableFuture<Void> runAsync(Runnable runnable) {
        ExecutionContext ctx = this.getExecutionContext();
        return CompletableFuture.runAsync(() -> {
            this.executionContext.set(ctx);
            runnable.run();
            this.executionContext.remove();
        });
    }
}
