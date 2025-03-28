package aptvantage.aptflow.examples;

import aptvantage.aptflow.api.RunnableWorkflow;

import static aptvantage.aptflow.api.WorkflowFunctions.activity;

public class ExampleWorkflowWithActivity implements RunnableWorkflow<Integer, String> {
    @Override
    public String execute(Integer param) {
        return activity("convert to string", () -> param.toString());
    }
}
