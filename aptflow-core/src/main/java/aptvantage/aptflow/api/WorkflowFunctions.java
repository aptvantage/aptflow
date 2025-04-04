package aptvantage.aptflow.api;

import com.google.common.flogger.FluentLogger;

import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class WorkflowFunctions {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private static StepFunctions SINGLETON = null;

    public static void initialize(StepFunctions stepFunctionsSingleton) {
        SINGLETON = stepFunctionsSingleton;
    }

    public static void sleep(String identifier, Duration duration) {
        SINGLETON.sleep(identifier, duration);
    }

    public static <R extends Serializable> CompletableFuture<R> async(Supplier<R> supplier) {
        return SINGLETON.async(supplier);
    }

    public static CompletableFuture<Void> async(Runnable runnable) {
        return SINGLETON.async(runnable);
    }

    public static <R extends Serializable> R activity(String activityName, Supplier<R> supplier) {
        return SINGLETON.activity(activityName, supplier);
    }

    public static void activity(String activityName, Runnable runnable) {
        SINGLETON.activity(activityName, runnable);
    }

    // TODO -- add an optional polling interval for condition reevaluation
    public static void awaitCondition(String conditionIdentifier, Supplier<Boolean> condition, Duration evaluationInternal) {
        SINGLETON.awaitCondition(conditionIdentifier, condition, evaluationInternal);
    }

    public static <S extends Serializable> S awaitSignal(String signalName, Class<S> returnType) {
        return SINGLETON.awaitSignal(signalName, returnType);
    }


}
