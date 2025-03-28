package aptvantage.aptflow.model;

import java.io.Serializable;
import java.time.Instant;

public record Workflow<I extends Serializable>(
        String id,
        String className,
        I input,
        Instant created
) {
}
