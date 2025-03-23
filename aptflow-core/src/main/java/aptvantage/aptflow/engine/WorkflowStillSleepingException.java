package aptvantage.aptflow.engine;

import java.time.Duration;

public class WorkflowStillSleepingException extends WorkflowSleepingException {
    private final Duration elapsedSleepTime;

    public WorkflowStillSleepingException(String identifier, Duration elapsedSleepTime, Duration napTime) {
        super(identifier, napTime);
        this.elapsedSleepTime = elapsedSleepTime;
    }

    public Duration getElapsedSleepTime() {
        return elapsedSleepTime;
    }
}
