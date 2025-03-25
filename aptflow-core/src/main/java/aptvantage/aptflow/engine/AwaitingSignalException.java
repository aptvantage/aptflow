package aptvantage.aptflow.engine;

public class AwaitingSignalException extends WorkflowPausedException {
    private final String signal;

    public AwaitingSignalException(String signal) {
        super();
        this.signal = signal;
    }

    public String getSignal() {
        return this.signal;
    }
}
