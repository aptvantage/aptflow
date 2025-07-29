package aptvantage.aptflow.engine;

public class ConditionTimeoutException extends RuntimeException {

    public ConditionTimeoutException(String conditionKey) {
        super("Condition [%s] timed out".formatted(conditionKey));
    }
}
