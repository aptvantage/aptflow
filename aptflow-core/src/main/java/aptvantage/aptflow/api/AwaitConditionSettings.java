package aptvantage.aptflow.api;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.function.Supplier;

public class AwaitConditionSettings {

    private final StepFunctions stepFunctions;

    private String stepName;
    private Duration evaluationInterval = Duration.of(1, ChronoUnit.MINUTES);
    private Duration timeout = Duration.of(365, ChronoUnit.DAYS);

    AwaitConditionSettings(StepFunctions stepFunctions) {
        this.stepFunctions = stepFunctions;
    }

    public AwaitConditionSettings stepName(String stepName) {
        checkNull(stepName, "stepName");
        this.stepName = stepName;
        return this;
    }

    public AwaitConditionSettings evaluationInterval(Duration evaluationInterval) {
        checkNull(evaluationInterval, "evaluationInterval");
        this.evaluationInterval = evaluationInterval;
        return this;
    }

    public AwaitConditionSettings timeout(Duration timeout) {
        checkNull(timeout, "timeout");
        this.timeout = timeout;
        return this;
    }

    public void awaitCondition(Supplier<Boolean> condition) {
        checkNull(this.stepName, "stepName");
        this.stepFunctions.awaitCondition(this, condition);
    }

    String stepName() {
        return stepName;
    }

    Duration evaluationInterval() {
        return evaluationInterval;
    }

    Duration timeout() {
        return timeout;
    }

    private static void checkNull(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException("%s must not be null".formatted(name));
        }
    }

}
