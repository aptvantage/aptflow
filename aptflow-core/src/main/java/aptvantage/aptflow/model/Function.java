package aptvantage.aptflow.model;

import java.time.Instant;

public record Function(String id, EventCategory category, Instant started, Instant completed) {
}
