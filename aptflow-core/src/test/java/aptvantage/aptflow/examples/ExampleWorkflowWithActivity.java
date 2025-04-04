package aptvantage.aptflow.examples;

import aptvantage.aptflow.api.RunnableWorkflow;
import aptvantage.aptflow.api.StepFunctions;

public class ExampleWorkflowWithActivity implements RunnableWorkflow<Integer, String> {

    private final StepFunctions steps;

    public ExampleWorkflowWithActivity(StepFunctions steps) {
        this.steps = steps;
    }

    @Override
    public String execute(Integer param) {
        return steps.activity("convert to string", () -> param.toString());
    }
}
