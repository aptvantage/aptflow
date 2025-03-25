package aptvantage.aptflow.examples;

import aptvantage.aptflow.api.RunnableWorkflow;

import static aptvantage.aptflow.api.WorkflowFunctions.activity;

public class ExampleWorkflowWithFailedRunnableActivity implements RunnableWorkflow<String, Integer> {
    @Override
    public String execute(Integer param) {

        String output = activity("convert int to string",
                () -> param.toString());

        activity("notify someone",
                () -> {
                    throw new RuntimeException("This Activity Failed");
                });
        return output;
    }
}
