package aptvantage.aptflow.api;

import aptvantage.aptflow.AptWorkflow;
import aptvantage.aptflow.engine.*;
import aptvantage.aptflow.model.Activity;
import aptvantage.aptflow.model.Condition;
import aptvantage.aptflow.model.Signal;
import aptvantage.aptflow.model.Sleep;
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

    public WorkflowFunctions(WorkflowExecutor workflowExecutor) {
        this.workflowExecutor = workflowExecutor;
    }

    public static void initialize(WorkflowExecutor workflowExecutor) {
        SINGLETON = new WorkflowFunctions(workflowExecutor);
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

    public static <T> T awaitSignal(String signalName, Class<T> returnType) {
        return SINGLETON._awaitSignal(signalName, returnType);
    }

    private void _awaitCondition(String conditionIdentifier, Supplier<Boolean> conditionSupplier, Duration evaluationInterval) {
        String workflowId = workflowExecutor.getExecutionContext().workflowId();
        String conditionKey = "condition::%s::%s".formatted(workflowId, conditionIdentifier);
        logger.atFine().log("processing condition [%s]", conditionKey);
        Condition condition = initializeCondition(workflowId, conditionIdentifier);
        if (condition.isSatisfied()) {
            logger.atInfo().log("skipping previously satisfied condition [%s]", conditionKey);
            return;
        }
        logger.atInfo().log("evaluating condition [%s]", conditionKey);
        if (conditionSupplier.get()) {
            logger.atInfo().log("satisfied condition [%s]", conditionKey);
            AptWorkflow.repository.conditionSatisfied(workflowId, conditionIdentifier);
            return;
        }
        logger.atInfo().log("Scheduling reevaluation of condition [%s] of workflow [%s] in [%s]", conditionIdentifier, workflowId, evaluationInterval);
        this.workflowExecutor.scheduleReevaluation(workflowId, conditionIdentifier, Instant.now().plus(evaluationInterval));

        throw new ConditionNotSatisfiedException(conditionIdentifier);

    }

    private Condition initializeCondition(String workflowId, String conditionIdentifier) {
        Condition condition = AptWorkflow.repository.getCondition(workflowId, conditionIdentifier);
        if (condition == null) {
            AptWorkflow.repository.newConditionWaiting(workflowId, conditionIdentifier);
            condition = AptWorkflow.repository.getCondition(workflowId, conditionIdentifier);
        }
        return condition;
    }

    private void _sleep(String identifier, Duration duration) {
        String workflowId = workflowExecutor.getExecutionContext().workflowId();
        logger.atFine().log("processing workflow sleep [%s::%s]".formatted(workflowId, identifier));
        Sleep sleep = AptWorkflow.repository.getSleep(workflowId, identifier);
        if (sleep == null) {
            AptWorkflow.repository.newSleepStarted(workflowId, identifier, duration);
            logger.atInfo().log("scheduling wake-up-call for sleep [%s::%s] in [%s]", workflowId, identifier, duration);
            this.workflowExecutor.scheduleWakeUp(workflowId, identifier, Instant.now().plus(duration));
            throw new WorkflowSleepingException(identifier, duration);
        }
        if (sleep.isCompleted()) {
            logger.atInfo().log("workflow has completed sleep [%s::%s]".formatted(workflowId, identifier));
            return;
        }

        // If a sleeping workflow was signaled, it needs to continue sleeping until its wakeup call
        Duration elapsedSleepTime = Duration.ofMillis(Instant.now().toEpochMilli() - sleep.started().timestamp().toEpochMilli());
        throw new WorkflowStillSleepingException(identifier, elapsedSleepTime, duration);
    }

    private <R extends Serializable> CompletableFuture<R> _async(Supplier<R> supplier) {
        return workflowExecutor.supplyAsync(supplier);
    }

    private CompletableFuture<Void> _async(Runnable runnable) {
        return workflowExecutor.runAsync(runnable);
    }

    private <T> T _awaitSignal(String signalName, Class<T> returnType) {
        String workflowId = workflowExecutor.getExecutionContext().workflowId();
        logger.atFine().log("processing signal [%s::%s]", workflowId, signalName);
        Signal signal = AptWorkflow.repository.getSignal(workflowId, signalName);
        if (signal == null) {
            logger.atInfo().log("waiting for signal [%s::%s]", workflowId, signalName);
            AptWorkflow.repository.newSignalWaiting(workflowId, signalName);
            throw new AwaitingSignalException(signalName);
        }
        if (signal.isReceived()) {
            return (T) signal.value();
        }

        // If a workflow woke from sleep, but is still waiting for a signal
        logger.atInfo().log("still waiting for signal [%s::%s]", workflowId, signalName);
        throw new AwaitingSignalException(signalName);
    }

    <R extends Serializable> R _activity(String activityName, Supplier<R> supplier) {
        String workflowId = workflowExecutor.getExecutionContext().workflowId();
        Activity activity = initActivity(workflowId, activityName);

        if (activityHasAlreadyExecuted(activity)) {
            return activityOutput(activity);
        }

        try {
            R output = supplier.get();
            completeActivity(activity, output);
            return output;
        } catch (Exception e) {
            rethrowIfWorkflowPausedException(e);
            failActivity(activity, e);
            throw new ActivityFailedException(activity, e);
        }
    }

    void _activity(String activityName, Runnable runnable) {
        String workflowId = workflowExecutor.getExecutionContext().workflowId();
        Activity activity = initActivity(workflowId, activityName);
        if (activityHasAlreadyExecuted(activity)) {
            return;
        }
        try {
            runnable.run();
            completeActivity(activity, null);
        } catch (Exception e) {
            rethrowIfWorkflowPausedException(e);
            failActivity(activity, e);
            throw new ActivityFailedException(activity, e);
        }
    }

    private void rethrowIfWorkflowPausedException(Exception e) {
        if (e instanceof WorkflowPausedException) {
            throw (WorkflowPausedException) e;
        }
    }

    private <R extends Serializable> R activityOutput(Activity activity) {
        return (R) AptWorkflow.repository.getActivity(activity.workflowRunId(), activity.name()).output();
    }

    private Activity initActivity(String workflowId, String activityName) {
        Activity activity = AptWorkflow.repository.getActivity(workflowId, activityName);
        logger.atFine().log("processing workflow activity [%s::%s]", workflowId, activityName);
        if (activity == null) {
            logger.atInfo().log("starting activity [%s::%s]", workflowId, activityName);
            AptWorkflow.repository.newActivityStarted(workflowId, activityName);
            activity = AptWorkflow.repository.getActivity(workflowId, activityName);
        }
        return activity;
    }

    private boolean activityHasAlreadyExecuted(Activity activity) {
        if (activity.isCompleted()) {
            logger.atInfo().log("skipping previously executed activity [%s]", activity.key());
            return true;
        }
        return false;
    }

    private <R extends Serializable> void completeActivity(Activity activity, R output) {
        AptWorkflow.repository.completeActivity(activity.workflowRunId(), activity.name(), output);
        logger.atInfo().log("completing activity [%s]", activity.key());
    }

    private void failActivity(Activity activity, Exception e) {
        AptWorkflow.repository.failActivity(activity);
        logger.atSevere().withCause(e).log("activity [%s] failed", activity.key());
    }

}
