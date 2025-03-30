package aptvantage.aptflow.model;

import java.io.Serializable;

public interface StepFunction<I extends Serializable, O extends Serializable> {
    WorkflowRun<I, O> getWorkflowRun();

    String getId();

    StepFunctionType getStepFunctionType();

    StepFunctionEvent<I, O> getStartedEvent();

    StepFunctionEvent<I, O> getCompletedEvent();

}
