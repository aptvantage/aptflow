package aptvantage.aptflow.examples;

import aptvantage.aptflow.api.RunnableWorkflow;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static aptvantage.aptflow.api.WorkflowFunctions.activity;
import static aptvantage.aptflow.api.WorkflowFunctions.awaitCondition;

public class ExampleWorkflowFailsFirstTime implements RunnableWorkflow<String, Integer> {

    private final TestCounterService testCounterService;

    public ExampleWorkflowFailsFirstTime(TestCounterService testCounterService) {
        this.testCounterService = testCounterService;
    }

    @Override
    public Integer execute(String testName) {

        activity("prior completed activity", () -> {
            //no-op
        });

        awaitCondition("prior condition", () -> true, Duration.of(3, ChronoUnit.SECONDS));

        int whichTime = activity("fails first time", () -> {
                    int testCount = testCounterService.incrementAndGetTestCount(testName);
                    if (testCount == 1) {
                        throw new RuntimeException("fails first time");
                    }
                    return testCount;
                }
        );
        return whichTime;
    }
}
