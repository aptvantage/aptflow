package aptvantage.aptflow.model;

import java.util.List;

public record WorkflowStatus(EventStatus status, List<Function> functions) {
    public WorkflowStatus {
        if (status == null) {
            throw new IllegalArgumentException("status cannot be null");
        }
    }

    public List<Function> activeFunctions() {
        return functions.stream()
                .filter((function) -> function.completed() == null)
                .toList();
    }

    public boolean isWaitingForSignal() {
        return activeFunctions().stream()
                .anyMatch((function) -> function.category() == EventCategory.SIGNAL);
    }

    public boolean isComplete() {
        return status.isTerminal();
    }

    public boolean hasFailed() {
        return status == EventStatus.FAILED;
    }

    public boolean hasStarted() {
       return status != EventStatus.SCHEDULED;
    }
}
