package aptvantage.aptflow.model;

import java.time.Instant;

public record Event(EventCategory category,
                    EventStatus status,
                    String functionId,
                    Instant timestamp) {
}
