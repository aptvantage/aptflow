package aptvantage.aptflow.model;

public enum StepFunctionEventStatus {
    STARTED, COMPLETED, FAILED, RECEIVED, WAITING, SCHEDULED, SATISFIED, TIMED_OUT;

    boolean isTerminal() {
        return FAILED == this || COMPLETED == this || TIMED_OUT == this || SATISFIED == this || RECEIVED == this;
    }
}
