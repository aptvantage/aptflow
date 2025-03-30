package aptvantage.aptflow.model;

import aptvantage.aptflow.engine.persistence.StateReader;

import java.io.Serializable;
import java.time.Instant;

public class StepFunctionEvent<I extends Serializable, O extends Serializable> {

    private final String id;
    private final String workflowRunId;
    private final StepFunctionType stepFunctionType;
    private final StepFunctionEventStatus stepFunctionEventStatus;
    private final Instant timestamp;
    private final String functionId;
    private final StateReader stateReader;

    public StepFunctionEvent(String id,
                             String workflowRunId,
                             StepFunctionType stepFunctionType,
                             StepFunctionEventStatus stepFunctionEventStatus,
                             Instant timestamp,
                             String functionId,
                             StateReader stateReader) {
        this.id = id;
        this.workflowRunId = workflowRunId;
        this.stepFunctionType = stepFunctionType;
        this.stepFunctionEventStatus = stepFunctionEventStatus;
        this.timestamp = timestamp;
        this.functionId = functionId;

        this.stateReader = stateReader;
    }

    public String getId() {
        return id;
    }

    public StepFunction<I, O> getStepFunction() {
        return stateReader.getStepFunctionForWorkflowRun(workflowRunId, functionId);
    }

    public StepFunctionType getFunctionType() {
        return stepFunctionType;
    }

    public StepFunctionEventStatus getStatus() {
        return stepFunctionEventStatus;
    }

    public WorkflowRun<I, O> getWorkflowRun() {
        return stateReader.getWorkflowRun(workflowRunId);
    }

    public Instant getTimestamp() {
        return timestamp;
    }
}
