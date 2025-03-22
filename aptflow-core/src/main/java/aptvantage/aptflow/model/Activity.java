package aptvantage.aptflow.model;

import java.io.Serializable;

public record Activity(
        String workflowId,
        String name,
        Serializable output,
        Event started,
        Event completed
) {
    public boolean isCompleted() {
        return this.completed != null && this.completed.timestamp() != null;
    }

    public String key() {
        return "%s::%s".formatted(workflowId, name);
    }
}
