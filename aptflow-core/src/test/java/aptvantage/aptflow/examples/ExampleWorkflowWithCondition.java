package aptvantage.aptflow.examples;

import aptvantage.aptflow.api.RunnableWorkflow;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static aptvantage.aptflow.api.WorkflowFunctions.awaitCondition;

public class ExampleWorkflowWithCondition implements RunnableWorkflow<String, Integer> {

    private static final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public String execute(Integer howManyTimes) {
        awaitCondition("run %s times".formatted(howManyTimes),
                () -> counter.incrementAndGet() >= howManyTimes,
                Duration.of(1, ChronoUnit.SECONDS));

        return String.valueOf(counter.get());
    }
}
