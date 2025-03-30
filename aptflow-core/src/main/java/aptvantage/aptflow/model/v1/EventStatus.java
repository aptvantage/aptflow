package aptvantage.aptflow.model.v1;

public enum EventStatus {
    STARTED, COMPLETED, FAILED, RECEIVED, WAITING, SCHEDULED, SATISFIED;

    boolean isTerminal() {
        return FAILED == this || COMPLETED == this;
    }
}
