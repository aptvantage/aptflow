package aptvantage.aptflow.engine;

public class ConditionNotSatisfiedException extends WorkflowPausedException {
    private final String identifier;

    public ConditionNotSatisfiedException(String identifier) {
        super();
        this.identifier = identifier;
    }

    public String getIdentifier() {
        return identifier;
    }
}
