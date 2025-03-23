package aptvantage.aptflow.model;

import java.util.List;

public record WorkflowStatus(EventStatus status, List<Function> activeFunctions, List<Function> failedFunctions) {
    public WorkflowStatus {
        if (status == null) {
            throw new IllegalArgumentException("status cannot be null");
        }
    }

    public boolean isComplete() {
        return status.isTerminal();
    }

}
