package aptvantage.aptflow.model;

import java.time.Duration;

public record Sleep(
        String workflowId,
        String identifier,
        Duration duration,
        Event started,
        Event completed
) {

    public boolean isCompleted() {
        return completed != null && completed.timestamp() != null;
    }

}
