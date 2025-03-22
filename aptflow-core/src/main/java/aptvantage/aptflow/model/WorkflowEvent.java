package aptvantage.aptflow.model;

import java.time.Instant;

public record WorkflowEvent(
        String id,
        String workflowId,
        EventCategory category,
        EventStatus status,
        Instant timestamp
) {
}
