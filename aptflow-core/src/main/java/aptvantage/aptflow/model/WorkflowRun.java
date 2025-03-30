package aptvantage.aptflow.model;

import aptvantage.aptflow.engine.persistence.StateReader;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;

public class WorkflowRun<I extends Serializable, O extends Serializable> {

    private final String id;

    private final String workflowId;

    private final String scheduledEventId;

    private final String startedEventId;

    private final String completedEventId;

    private final O output;

    private final Instant archived;
    private final StateReader stateReader;


    public WorkflowRun(
            String id,
            String workflowId,
            String scheduledEventId,
            String startedEventId,
            String completedEventId,
            O output,
            Instant archived,
            StateReader stateReader) {
        this.id = id;
        this.workflowId = workflowId;
        this.scheduledEventId = scheduledEventId;
        this.startedEventId = startedEventId;
        this.completedEventId = completedEventId;
        this.output = output;
        this.archived = archived;
        this.stateReader = stateReader;
    }

    public String getId() {
        return id;
    }

    public Workflow<I, O> getWorkflow() {
        return stateReader.getWorkflow(workflowId);
    }

    public O getOutput() {
        return output;
    }

    public StepFunctionEvent<I, O> getScheduledEvent() {
        return stateReader.getStepFunctionEvent(scheduledEventId);
    }

    public StepFunctionEvent<I, O> getStartedEvent() {
        return stateReader.getStepFunctionEvent(startedEventId);
    }

    public StepFunctionEvent<I, O> getCompletedEvent() {
        return stateReader.getStepFunctionEvent(completedEventId);
    }

    public Instant getArchived() {
        return archived;
    }

    public List<StepFunction<I, O>> getFunctions() {
        return stateReader.getFunctionsForWorkflowRun(id);
    }

    public List<StepFunctionEvent<I, O>> getFunctionEvents() {
        return stateReader.getStepFunctionEventsForWorkflowRun(id);
    }
}
