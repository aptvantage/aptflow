package aptvantage.aptflow.examples;

import aptvantage.aptflow.api.RunnableWorkflow;

public class ExampleSimpleWorkflow implements RunnableWorkflow<String, Integer> {
    public String execute(Integer param) {
        System.out.println("executing with param [%s]".formatted(param));
        return param.toString();
    }
}
