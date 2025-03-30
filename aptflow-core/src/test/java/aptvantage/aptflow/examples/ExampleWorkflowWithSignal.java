package aptvantage.aptflow.examples;

import aptvantage.aptflow.api.RunnableWorkflow;

import static aptvantage.aptflow.api.WorkflowFunctions.awaitSignal;

public class ExampleWorkflowWithSignal implements RunnableWorkflow<Integer, String> {
    @Override
    public String execute(Integer param) {
        Integer multiplyBy = awaitSignal("multiplyBy", Integer.class);

        return String.valueOf(param * multiplyBy);
    }
}
