package aptvantage.aptflow.examples;

import aptvantage.aptflow.api.RunnableWorkflow;

import java.time.Duration;

import static aptvantage.aptflow.api.WorkflowFunctions.sleep;

public class ExampleWorkflowWithSleep implements RunnableWorkflow<Integer, String> {
    @Override
    public String execute(Integer param) {
        sleep("nap for 3 seconds", Duration.ofSeconds(3));
        return param.toString();
    }
}
