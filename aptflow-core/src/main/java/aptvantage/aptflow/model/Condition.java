package aptvantage.aptflow.model;

public record Condition(
        String workflowId,
        String identifier,
        Event waiting,
        Event satisfied
) {
    public boolean isSatisfied() {
        return this.satisfied != null && this.satisfied.timestamp() != null;
    }

}
