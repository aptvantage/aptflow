package aptvantage.aptflow.model.v1;

import java.time.Instant;

public record Event(EventCategory category,
                    EventStatus status,
                    String functionId,
                    Instant timestamp) {
}
