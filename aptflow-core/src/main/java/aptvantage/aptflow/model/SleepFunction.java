package aptvantage.aptflow.model;

import aptvantage.aptflow.engine.persistence.StateReader;

import java.io.Serializable;
import java.time.Duration;

public class SleepFunction<I extends Serializable, O extends Serializable> implements StepFunction<I, O> {

    private final String workflowRunId;
    private final String identifier;
    private final String startedEventId;
    private final String completedEventId;
    private final Long durationInMillis;
    private final StateReader stateReader;

    public SleepFunction(
            String workflowRunId,
            String identifier,
            String startedEventId,
            String completedEventId,
            Long durationInMillis,
            StateReader stateReader
    ) {
        this.workflowRunId = workflowRunId;
        this.identifier = identifier;
        this.startedEventId = startedEventId;
        this.completedEventId = completedEventId;
        this.durationInMillis = durationInMillis;
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
        return StepFunctionType.SLEEP;
    }

    @Override
    public StepFunctionEvent<I, O> getStartedEvent() {
        return stateReader.getStepFunctionEvent(startedEventId);
    }

    @Override
    public StepFunctionEvent<I, O> getCompletedEvent() {
        return stateReader.getStepFunctionEvent(completedEventId);
    }

    public Duration getDuration() {
        return Duration.ofMillis(durationInMillis);
    }

    public boolean isCompleted() {
        return completedEventId != null;
    }
}
