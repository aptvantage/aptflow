package aptvantage.aptflow.model;

import aptvantage.aptflow.engine.persistence.StateReader;

import java.io.Serializable;

public class ActivityFunction<I extends Serializable, O extends Serializable, A extends Serializable> implements StepFunction<I, O> {

    private final String workflowRunId;
    private final String name;
    private final String startedEventId;
    private final String completedEventId;
    private final A output;
    private final StateReader stateReader;

    public ActivityFunction(
            String workflowRunId,
            String name,
            String startedEventId,
            String completedEventId,
            A output,
            StateReader stateReader
    ) {
        this.workflowRunId = workflowRunId;
        this.name = name;
        this.startedEventId = startedEventId;
        this.completedEventId = completedEventId;
        this.output = output;
        this.stateReader = stateReader;
    }

    @Override
    public WorkflowRun<I, O> getWorkflowRun() {
        return stateReader.getWorkflowRun(workflowRunId);
    }

    @Override
    public String getId() {
        return name;
    }

    public String getName() {
        return name;
    }

    @Override
    public StepFunctionType getStepFunctionType() {
        return StepFunctionType.ACTIVITY;
    }

    @Override
    public StepFunctionEvent<I, O> getStartedEvent() {
        return stateReader.getStepFunctionEvent(startedEventId);
    }

    @Override
    public StepFunctionEvent<I, O> getCompletedEvent() {
        return stateReader.getStepFunctionEvent(completedEventId);
    }

    public A getOutput() {
        return output;
    }

    public String getKey() {
        return "%s::%s".formatted(workflowRunId, name);
    }
}
