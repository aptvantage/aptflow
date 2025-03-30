package aptvantage.aptflow.model;

import aptvantage.aptflow.engine.persistence.StateReader;

import java.io.Serializable;

public class SignalFunction<I extends Serializable, O extends Serializable, S extends Serializable> implements StepFunction<I, O> {

    private final String workflowRunId;
    private final String name;
    private final String waitingEventId;
    private final String receivedEventId;
    private final S value;
    private final StateReader stateReader;

    public SignalFunction(
            String workflowRunId,
            String name,
            String waitingEventId,
            String receivedEventId,
            S value,
            StateReader stateReader
    ) {
        this.workflowRunId = workflowRunId;
        this.name = name;
        this.waitingEventId = waitingEventId;
        this.receivedEventId = receivedEventId;
        this.value = value;
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

    @Override
    public StepFunctionType getStepFunctionType() {
        return StepFunctionType.SIGNAL;
    }

    @Override
    public StepFunctionEvent<I, O> getStartedEvent() {
        return stateReader.getStepFunctionEvent(waitingEventId);
    }

    @Override
    public StepFunctionEvent<I, O> getCompletedEvent() {
        return stateReader.getStepFunctionEvent(receivedEventId);
    }

    public S getValue() {
        return value;
    }

    public boolean isReceived() {
        return receivedEventId != null;
    }
}
