package aptvantage.aptflow.examples;

import aptvantage.aptflow.api.RunnableWorkflow;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static aptvantage.aptflow.api.WorkflowFunctions.*;

public class ExampleWorkflowWithNestedActivities implements RunnableWorkflow<Integer, String> {

    static final AtomicInteger execCount = new AtomicInteger(0);

    @Override
    public Integer execute(String param) {

        activity("1", () -> {
            System.out.println("starting main activity");

            activity("1.1", () -> {
                System.out.println("starting 1.1");

                sleep("1.1.1", Duration.ofSeconds(2));

                activity("1.1.2", () -> {

                    awaitCondition("1.1.2.1", () -> execCount.getAndIncrement() > 2, Duration.ofSeconds(3));

                    System.out.println("completing 1.1.2");
                });

                activity("1.1.3", () -> {
                    System.out.println("staring and completing 1.1.3");
                });

                System.out.println("ending 1.1");
            });
            System.out.println("ending 1 (main activity)");
        });

        return Integer.parseInt(param);

    }
}
