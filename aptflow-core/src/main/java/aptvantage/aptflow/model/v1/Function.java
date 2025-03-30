package aptvantage.aptflow.model.v1;

import java.time.Instant;

public record Function(String id, EventCategory category, Instant started, Instant completed) {
}
