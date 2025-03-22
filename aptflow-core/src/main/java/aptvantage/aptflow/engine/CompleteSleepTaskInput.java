package aptvantage.aptflow.engine;

import java.io.Serializable;

public record CompleteSleepTaskInput(
        String workflowId,
        String sleepIdentifier
) implements Serializable {
}
