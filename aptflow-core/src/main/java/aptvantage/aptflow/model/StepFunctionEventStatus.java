package aptvantage.aptflow.model;

public enum StepFunctionEventStatus {
    STARTED, COMPLETED, FAILED, RECEIVED, WAITING, SCHEDULED, SATISFIED;

    boolean isTerminal() {
        return FAILED == this || COMPLETED == this;
    }
}
