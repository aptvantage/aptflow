package aptvantage.aptflow.model;

import java.time.Instant;

public record Event(String category, String status, String functionId, Instant timestamp) {
}
