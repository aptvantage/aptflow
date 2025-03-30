package aptvantage.aptflow.engine;

import java.io.Serializable;

public record RunWorkflowTaskInput(String workflowRunId) implements Serializable {
}
