package aptvantage.aptflow.model;

import aptvantage.aptflow.engine.persistence.StateReader;

import java.io.Serializable;

public class ConditionFunction<I extends Serializable, O extends Serializable> implements StepFunction<I, O> {

    private final String workflowRunId;
    private final String identifier;
    private final String waitingEventId;
    private final String satisfiedEventId;
    private final StateReader stateReader;


    public ConditionFunction(
            String workflowRunId,
            String identifier,
            String waitingEventId,
            String satisfiedEventId,
            StateReader stateReader
    ) {
        this.workflowRunId = workflowRunId;
        this.identifier = identifier;
        this.waitingEventId = waitingEventId;
        this.satisfiedEventId = satisfiedEventId;
        this.stateReader = stateReader;
    }

    @Override
    public WorkflowRun<I, O> getWorkflowRun() {
        return stateReader.getWorkflowRun(workflowRunId);
    }

    @Override
    public String getId() {
        return identifier;
    }

    @Override
    public StepFunctionType getStepFunctionType() {
        return StepFunctionType.CONDITION;
    }

    @Override
    public StepFunctionEvent<I, O> getStartedEvent() {
        return stateReader.getStepFunctionEvent(waitingEventId);
    }

    @Override
    public StepFunctionEvent<I, O> getCompletedEvent() {
        return stateReader.getStepFunctionEvent(satisfiedEventId);
    }

    public boolean isSatisfied() {
        return satisfiedEventId != null;
    }
}
