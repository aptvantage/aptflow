package aptvantage.aptflow.engine;

import java.io.Serializable;

public record StartWorkflowTaskInput(String workflowId) implements Serializable {
}
