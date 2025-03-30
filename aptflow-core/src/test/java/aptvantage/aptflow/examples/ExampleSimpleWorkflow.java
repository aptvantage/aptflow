package aptvantage.aptflow.examples;

import aptvantage.aptflow.api.RunnableWorkflow;

public class ExampleSimpleWorkflow implements RunnableWorkflow<Integer, String> {
    public String execute(Integer param) {
        System.out.printf("executing with param [%s]%n", param);
        return param.toString();
    }
}
