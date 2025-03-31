package aptvantage.aptflow.model;

import java.io.Serializable;

public interface StepFunction<I extends Serializable, O extends Serializable> {
    WorkflowRun<I, O> getWorkflowRun();

    String getId();

    StepFunctionType getStepFunctionType();

    StepFunctionEvent<I, O> getStartedEvent();

    StepFunctionEvent<I, O> getCompletedEvent();

    default boolean hasCompleted() {
        StepFunctionEvent<I, O> completedEvent = getCompletedEvent();
        return completedEvent != null && completedEvent.getStatus().isTerminal();
    }

    default boolean hasFailed() {
        StepFunctionEvent<I, O> completedEvent = getCompletedEvent();
        return completedEvent != null && completedEvent.getStatus() == StepFunctionEventStatus.FAILED;
    }

}
