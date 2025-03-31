package aptvantage.aptflow.engine;

import aptvantage.aptflow.api.RunnableWorkflow;
import aptvantage.aptflow.engine.persistence.StateReader;
import aptvantage.aptflow.engine.persistence.StateWriter;
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
    private final StateWriter stateWriter;

    private final Set<Object> workflowDependencies;

    private final CompleteSleepTask completeSleepTask;
    private final StateReader stateReader;
    private final ResumeStartedWorkflowTask resumeStartedWorkflowTask;
    private final SignalWorkflowTask signalWorkflowTask;
    private final StartWorkflowTask startWorkflowTask;

    public WorkflowExecutor(
            DataSource dataSource,
            StateWriter stateWriter,
            Set<Object> workflowDependencies,
            StateReader stateReader
    ) {
        this.completeSleepTask = new CompleteSleepTask(stateWriter, this);
        this.stateReader = stateReader;
        this.resumeStartedWorkflowTask = new ResumeStartedWorkflowTask(this);
        this.startWorkflowTask = new StartWorkflowTask(stateWriter, this);
        this.signalWorkflowTask = new SignalWorkflowTask(stateWriter, this);
        this.scheduler = Scheduler
                .create(dataSource,
                        startWorkflowTask,
                        completeSleepTask,
                        signalWorkflowTask,
                        resumeStartedWorkflowTask)
                .pollingInterval(Duration.ofSeconds(1))
                .enableImmediateExecution()
                .build();
        this.stateWriter = stateWriter;
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

    void executeWorkflow(String workflowRunId) {
        WorkflowRun workflowRun = stateReader.getWorkflowRun(workflowRunId);

        try {
            executionContext.set(new ExecutionContext(workflowRunId));
            RunnableWorkflow instance = instantiate(workflowRun.getWorkflow().getClassName());
            Serializable output = instance.execute(workflowRun.getWorkflow().getInput());
            this.stateWriter.workflowRunCompleted(workflowRunId, output, Instant.now());
            logger.atInfo().log("Workflow [%s] is complete", workflowRunId);
        } catch (AwaitingSignalException e) {
            logger.atInfo().log("Pausing execution of workflow [%s] to wait for signal [%s]", workflowRunId, e.getSignal());
        } catch (WorkflowStillSleepingException e) {
            logger.atInfo().log("Workflow [%s] has been sleeping [%s] for [%s] out of [%s]", workflowRunId, e.getIdentifier(), e.getElapsedSleepTime(), e.getNapTime());
        } catch (WorkflowSleepingException e) {
            logger.atInfo().log("Pausing execution of workflow [%s] to sleep [%s] for [%s]", workflowRunId, e.getIdentifier(), e.getNapTime());
        } catch (ConditionNotSatisfiedException e) {
            logger.atInfo().log("Pausing execution of workflow [%s] because condition [%s] is not satisfied", workflowRunId, e.getIdentifier());
        } catch (Exception e) {
            // TODO -- save some kind of Failure data with the failed workflow
            logger.atSevere().withCause(e).log("Workflow [%s] execution failed", workflowRunId);
            this.stateWriter.failWorkflowRun(workflowRunId, Instant.now());
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

    public <T extends Serializable> void signalWorkflowRun(String workflowRunId, String signalName, T signalValue) {
        logger.atInfo().log("received signal [%s::%s]", workflowRunId, signalName);
        //TODO -- validate the signalValue is of the expected type
        TaskInstance<SignalWorkflowTaskInput> instance = signalWorkflowTask.instance(
                "signal::%s::%s".formatted(workflowRunId, signalName),
                new SignalWorkflowTaskInput(workflowRunId, signalName, signalValue));
        scheduler.schedule(instance, Instant.now());
        //TODO - track signal sent in addition to signal received
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> this.stateReader.getSignalFunction(workflowRunId, signalName).isReceived());
    }

    public <I extends Serializable, O extends Serializable> void runWorkflow(Class<? extends RunnableWorkflow<I, O>> workflowClass, I workflowParam, String workflowId) {
        logger.atInfo().log("scheduling run for new workflow [%s] of type [%s]", workflowId, workflowClass.getName());
        String workflowRunId = this.stateWriter.scheduleRunForNewWorkflow(workflowId, workflowClass, workflowParam);
        startRun(workflowRunId);
    }

    public void reRunWorkflowFromStart(String workflowId) {
        logger.atInfo().log("scheduling re-run of existing workflow [%s]", workflowId);
        String workflowRunId = stateWriter.scheduleNewRunForExistingWorkflow(workflowId, false);
        startRun(workflowRunId);
    }

    public void reRunWorkflowFromFailed(String workflowId) {
        // TODO - test this should fail if workflow does not exist
        // TODO - test this should fail if existing workflow's latest run is not in a failed state
        logger.atInfo().log("scheduling re-run from point of failure for workflow [%s]", workflowId);
        String workflowRunId = stateWriter.scheduleNewRunForExistingWorkflow(workflowId, true);
        startRun(workflowRunId);
    }

    private void startRun(String workflowRunId) {
        TaskInstance<RunWorkflowTaskInput> instance = startWorkflowTask.instance(
                "workflow::%s".formatted(workflowRunId),
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
