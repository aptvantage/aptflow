package aptvantage.aptflow.model;

import aptvantage.aptflow.engine.persistence.StateReader;

import java.io.Serializable;
import java.util.List;

public class Workflow<I extends Serializable, O extends Serializable> {

    private final String id;

    private final String className;

    private final I input;
    private final StateReader stateReader;

    public Workflow(String id, String className, I input, StateReader stateReader) {
        this.id = id;
        this.className = className;
        this.input = input;
        this.stateReader = stateReader;
    }

    public String getId() {
        return id;
    }

    public String getClassName() {
        return className;
    }

    public I getInput() {
        return input;
    }

    public List<WorkflowRun<I, O>> getWorkflowRuns() {
        return stateReader.getRunsForWorkflow(id);
    }
}
