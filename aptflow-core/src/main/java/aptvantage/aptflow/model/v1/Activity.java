package aptvantage.aptflow.model.v1;

import java.io.Serializable;

public record Activity(
        String workflowRunId,
        String name,
        Serializable output,
        Event started,
        Event completed
) {
    public boolean isCompleted() {
        return this.completed != null && this.completed.timestamp() != null;
    }

    public String key() {
        return "%s::%s".formatted(workflowRunId, name);
    }
}
