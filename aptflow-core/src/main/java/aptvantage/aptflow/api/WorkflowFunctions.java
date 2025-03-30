package aptvantage.aptflow.api;

import aptvantage.aptflow.engine.*;
import aptvantage.aptflow.engine.persistence.StateReader;
import aptvantage.aptflow.engine.persistence.StateWriter;
import aptvantage.aptflow.model.ActivityFunction;
import aptvantage.aptflow.model.ConditionFunction;
import aptvantage.aptflow.model.SignalFunction;
import aptvantage.aptflow.model.SleepFunction;
import com.google.common.flogger.FluentLogger;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class WorkflowFunctions {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private static WorkflowFunctions SINGLETON = null;
    private final WorkflowExecutor workflowExecutor;
    private final StateReader stateReader;
    private final StateWriter stateWriter;

    public WorkflowFunctions(
            WorkflowExecutor workflowExecutor,
            StateReader stateReader,
            StateWriter stateWriter
    ) {
        this.workflowExecutor = workflowExecutor;
        this.stateReader = stateReader;
        this.stateWriter = stateWriter;
    }

    public static void initialize(
            WorkflowExecutor workflowExecutor,
            StateReader stateReader,
            StateWriter stateWriter
    ) {
        SINGLETON = new WorkflowFunctions(workflowExecutor, stateReader, stateWriter);
    }

    public static void sleep(String identifier, Duration duration) {
        SINGLETON._sleep(identifier, duration);
    }

    public static <R extends Serializable> CompletableFuture<R> async(Supplier<R> supplier) {
        return SINGLETON._async(supplier);
    }

    public static CompletableFuture<Void> async(Runnable runnable) {
        return SINGLETON._async(runnable);
    }

    public static <R extends Serializable> R activity(String activityName, Supplier<R> supplier) {
        return SINGLETON._activity(activityName, supplier);
    }

    public static void activity(String activityName, Runnable runnable) {
        SINGLETON._activity(activityName, runnable);
    }

    // TODO -- add an optional polling interval for condition reevaluation
    public static void awaitCondition(String conditionIdentifier, Supplier<Boolean> condition, Duration evaluationInternal) {
        SINGLETON._awaitCondition(conditionIdentifier, condition, evaluationInternal);
    }

    public static <S extends Serializable> S awaitSignal(String signalName, Class<S> returnType) {
        return SINGLETON._awaitSignal(signalName, returnType);
    }

    private void _awaitCondition(String conditionIdentifier, Supplier<Boolean> conditionSupplier, Duration evaluationInterval) {
        String workflowRunId = workflowExecutor.getExecutionContext().workflowRunId();
        String conditionKey = "condition::%s::%s".formatted(workflowRunId, conditionIdentifier);
        logger.atFine().log("processing condition [%s]", conditionKey);
        ConditionFunction<? extends Serializable, ? extends Serializable> conditionFunction = initializeCondition(workflowRunId, conditionIdentifier);
        if (conditionFunction.isSatisfied()) {
            logger.atInfo().log("skipping previously satisfied condition [%s]", conditionKey);
            return;
        }
        logger.atInfo().log("evaluating condition [%s]", conditionKey);
        if (conditionSupplier.get()) {
            logger.atInfo().log("satisfied condition [%s]", conditionKey);
            stateWriter.conditionSatisfied(workflowRunId, conditionIdentifier);
            return;
        }
        logger.atInfo().log("Scheduling reevaluation of condition [%s] of workflow [%s] in [%s]", conditionIdentifier, workflowRunId, evaluationInterval);
        this.workflowExecutor.scheduleReevaluation(workflowRunId, conditionIdentifier, Instant.now().plus(evaluationInterval));

        throw new ConditionNotSatisfiedException(conditionIdentifier);

    }

    private <I extends Serializable, O extends Serializable>
    ConditionFunction<I, O> initializeCondition(String workflowRunId, String conditionIdentifier) {
        ConditionFunction<I, O> conditionFunction = stateReader.getConditionFunction(workflowRunId, conditionIdentifier);
        if (conditionFunction == null) {
            stateWriter.newConditionWaiting(workflowRunId, conditionIdentifier);
            conditionFunction = stateReader.getConditionFunction(workflowRunId, conditionIdentifier);
        }
        return conditionFunction;
    }

    private void _sleep(String identifier, Duration duration) {
        String workflowRunId = workflowExecutor.getExecutionContext().workflowRunId();
        logger.atFine().log("processing workflow sleep [%s::%s]".formatted(workflowRunId, identifier));
        SleepFunction<? extends Serializable, ? extends Serializable> sleepFunction = stateReader.getSleepFunction(workflowRunId, identifier);
        if (sleepFunction == null) {
            stateWriter.newSleepStarted(workflowRunId, identifier, duration);
            logger.atInfo().log("scheduling wake-up-call for sleep [%s::%s] in [%s]", workflowRunId, identifier, duration);
            this.workflowExecutor.scheduleWakeUp(workflowRunId, identifier, Instant.now().plus(duration));
            throw new WorkflowSleepingException(identifier, duration);
        }
        if (sleepFunction.isCompleted()) {
            logger.atInfo().log("workflow has completed sleep [%s::%s]".formatted(workflowRunId, identifier));
            return;
        }

        // If a sleeping workflow was signaled, it needs to continue sleeping until its wakeup call
        Duration elapsedSleepTime = Duration.ofMillis(Instant.now().toEpochMilli() - sleepFunction.getStartedEvent().getTimestamp().toEpochMilli());
        throw new WorkflowStillSleepingException(identifier, elapsedSleepTime, duration);
    }

    private <R extends Serializable> CompletableFuture<R> _async(Supplier<R> supplier) {
        return workflowExecutor.supplyAsync(supplier);
    }

    private CompletableFuture<Void> _async(Runnable runnable) {
        return workflowExecutor.runAsync(runnable);
    }

    private <I extends Serializable, O extends Serializable, S extends Serializable>
    S _awaitSignal(String signalName, Class<S> returnType) {
        String workflowRunId = workflowExecutor.getExecutionContext().workflowRunId();
        logger.atFine().log("processing signal [%s::%s]", workflowRunId, signalName);
        SignalFunction<I, O, S> signalFunction = stateReader.getSignalFunction(workflowRunId, signalName);
        if (signalFunction == null) {
            logger.atInfo().log("waiting for signal [%s::%s]", workflowRunId, signalName);
            stateWriter.newSignalWaiting(workflowRunId, signalName);
            throw new AwaitingSignalException(signalName);
        }
        if (signalFunction.isReceived()) {
            return signalFunction.getValue();
        }

        // If a workflow woke from sleep, but is still waiting for a signal
        logger.atInfo().log("still waiting for signal [%s::%s]", workflowRunId, signalName);
        throw new AwaitingSignalException(signalName);
    }

    <A extends Serializable> A _activity(String activityName, Supplier<A> supplier) {
        String workflowRunId = workflowExecutor.getExecutionContext().workflowRunId();
        ActivityFunction<? extends Serializable, ? extends Serializable, A> activityFunction = initActivity(workflowRunId, activityName);

        if (activityHasAlreadyExecuted(activityFunction)) {
            return activityFunction.getOutput();
        }

        try {
            A output = supplier.get();
            completeActivity(activityFunction, output);
            return output;
        } catch (Exception e) {
            rethrowIfWorkflowPausedException(e);
            failActivity(activityFunction, e);
            throw new ActivityFailedException(activityFunction, e);
        }
    }

    void _activity(String activityName, Runnable runnable) {
        String workflowRunId = workflowExecutor.getExecutionContext().workflowRunId();
        ActivityFunction<? extends Serializable, ? extends Serializable, ? extends Serializable>
                activityFunction = initActivity(workflowRunId, activityName);
        if (activityHasAlreadyExecuted(activityFunction)) {
            return;
        }
        try {
            runnable.run();
            completeActivity(activityFunction, null);
        } catch (Exception e) {
            rethrowIfWorkflowPausedException(e);
            failActivity(activityFunction, e);
            throw new ActivityFailedException(activityFunction, e);
        }
    }

    private void rethrowIfWorkflowPausedException(Exception e) {
        if (e instanceof WorkflowPausedException) {
            throw (WorkflowPausedException) e;
        }
    }

    private <I extends Serializable, O extends Serializable, A extends Serializable>
    ActivityFunction<I, O, A> initActivity(String workflowRunId, String activityName) {
        ActivityFunction<I, O, A> activityFunction = stateReader.getActivityFunction(workflowRunId, activityName);
        logger.atFine().log("processing workflow activity [%s::%s]", workflowRunId, activityName);
        if (activityFunction == null) {
            logger.atInfo().log("starting activity [%s::%s]", workflowRunId, activityName);
            stateWriter.newActivityStarted(workflowRunId, activityName);
            activityFunction = stateReader.getActivityFunction(workflowRunId, activityName);
        }
        return activityFunction;
    }

    private <I extends Serializable, O extends Serializable, A extends Serializable>
    boolean activityHasAlreadyExecuted(ActivityFunction<I, O, A> activity) {
        if (activity.isCompleted()) {
            logger.atInfo().log("skipping previously executed activity [%s]", activity.getKey());
            return true;
        }
        return false;
    }

    private <I extends Serializable, O extends Serializable, A extends Serializable>
    void completeActivity(ActivityFunction<I, O, A> activity, A output) {
        stateWriter.completeActivity(activity.getWorkflowRun().getId(), activity.getName(), output);
        logger.atInfo().log("completing activity [%s]", activity.getKey());
    }

    private <I extends Serializable, O extends Serializable, A extends Serializable> void failActivity(ActivityFunction<I, O, A> activity, Exception e) {
        stateWriter.failActivity(activity.getWorkflowRun().getId(), activity.getName());
        logger.atSevere().withCause(e).log("activity [%s] failed", activity.getKey());
    }

}
