package aptvantage.aptflow.engine;

import java.io.Serializable;

public record WaitForConditionTaskInput(
        String workflowClassName,
        String workflowId
) implements Serializable {
}
