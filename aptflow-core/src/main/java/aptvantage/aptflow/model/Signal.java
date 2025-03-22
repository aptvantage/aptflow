package aptvantage.aptflow.model;

public record Signal(
        String workflowId,
        String name,
        Object value,
        Event waiting,
        Event received
) {
    public boolean isReceived() {
        return this.received != null && this.received.timestamp() != null;
    }
}
