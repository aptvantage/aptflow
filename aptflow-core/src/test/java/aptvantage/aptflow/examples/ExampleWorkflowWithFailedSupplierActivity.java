package aptvantage.aptflow.examples;

import aptvantage.aptflow.api.RunnableWorkflow;

import static aptvantage.aptflow.api.WorkflowFunctions.activity;

public class ExampleWorkflowWithFailedSupplierActivity implements RunnableWorkflow<String, Integer> {
    @Override
    public String execute(Integer param) {

        String output = activity("convert int to string",
                () -> {
                    throw new RuntimeException("activity failed");
                });

        return output;
    }
}
