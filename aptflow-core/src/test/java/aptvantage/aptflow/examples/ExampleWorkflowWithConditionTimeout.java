package aptvantage.aptflow.examples;

import aptvantage.aptflow.api.RunnableWorkflow;
import aptvantage.aptflow.api.StepFunctions;

import java.time.Duration;

public class ExampleWorkflowWithConditionTimeout implements RunnableWorkflow<Integer, String> {

    private final StepFunctions steps;

    public ExampleWorkflowWithConditionTimeout(StepFunctions steps) {
        this.steps = steps;
    }

    @Override
    public String execute(Integer param) {
        steps.withConditionSettings()
                .stepName("await a timeout")
                .evaluationInterval(Duration.ofSeconds(2))
                .timeout(Duration.ofSeconds(5))
                .awaitCondition(() -> false);
        return "this will never return";
    }
}
