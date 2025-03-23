package aptvantage.aptflow.engine;

import aptvantage.aptflow.api.RunnableWorkflow;
import aptvantage.aptflow.engine.persistence.WorkflowRepository;
import aptvantage.aptflow.model.Workflow;
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
    private final RunWorkflowTask runWorkflowTask;
    private final SignalWorkflowTask signalWorkflowTask;

    public WorkflowExecutor(DataSource dataSource, WorkflowRepository workflowRepository, Set<Object> workflowDependencies) {
        this.completeSleepTask = new CompleteSleepTask(workflowRepository, this);
        this.runWorkflowTask = new RunWorkflowTask(workflowRepository, this);
        this.signalWorkflowTask = new SignalWorkflowTask(workflowRepository, this);
        this.scheduler = Scheduler
                .create(dataSource,
                        completeSleepTask,
                        signalWorkflowTask,
                        runWorkflowTask)
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
        Workflow workflow = this.workflowRepository.getWorkflow(workflowId);
        try {
            RunnableWorkflow instance = instantiate(workflow.className());
            executionContext.set(new ExecutionContext(workflowId));
            Object output = instance.execute(workflow.input());
            executionContext.remove();
            this.workflowRepository.workflowCompleted(workflowId, output);
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
            //TODO -- handle unexpected/unhandled failure encountered while evaluating workflow
            // this should not re-throw an exception, it should handle them all
            throw new RuntimeException(e);
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
        StartWorkflowTaskInput input = new StartWorkflowTaskInput(workflowId);
        String taskInstanceId = "%s::%s::%s".formatted(workflowId, conditionId, UUID.randomUUID().toString());
        scheduler.schedule(this.runWorkflowTask.instance(
                taskInstanceId, input), resumptionTime);
    }

    public void scheduleWakeUp(String workflowId, String sleepId, Instant wakeupTime) {
        CompleteSleepTaskInput input = new CompleteSleepTaskInput(workflowId, sleepId);
        scheduler.schedule(this.completeSleepTask.instance(
                "sleep::%s::%s".formatted(workflowId, sleepId), input
        ), wakeupTime);
    }

    public <T extends Serializable> void signalWorkflow(String workflowId, String signalName, T signalValue) {
        logger.atInfo().log("received signal [%s::%s]", workflowId, signalName);
        TaskInstance<SignalWorkflowTaskInput> instance = signalWorkflowTask.instance(
                "signal::%s::%s".formatted(workflowId, signalName),
                new SignalWorkflowTaskInput(workflowId, signalName, signalValue));
        scheduler.schedule(instance, Instant.now());
        //TODO - track signal sent in addition to signal received
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> this.workflowRepository.isSignalReceived(workflowId, signalName));
    }

    public <P extends Serializable> void runWorkflow(Class<? extends RunnableWorkflow<?, P>> workflowClass, P workflowParam, String workflowId) {
        logger.atInfo().log("scheduling new workflow [%s] of type [%s]", workflowId, workflowClass.getName());
        this.workflowRepository.newWorkflowScheduled(workflowId, workflowClass, workflowParam);
        TaskInstance<StartWorkflowTaskInput> instance = runWorkflowTask.instance(
                "workflow::%s".formatted(workflowId),
                new StartWorkflowTaskInput(workflowId));
        scheduler.schedule(instance,
                Instant.now());
        // TODO -- probably don't need to wait here as we already have a "scheduled" state of a workflow
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> this.workflowRepository.hasWorkflowStarted(workflowId));
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
