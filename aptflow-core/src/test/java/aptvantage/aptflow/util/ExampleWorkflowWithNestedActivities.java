package aptvantage.aptflow.util;

import aptvantage.aptflow.api.RunnableWorkflow;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static aptvantage.aptflow.api.WorkflowFunctions.*;

public class ExampleWorkflowWithNestedActivities implements RunnableWorkflow<Integer, String> {

    static AtomicInteger execCount = new AtomicInteger(0);
    @Override
    public Integer execute(String param) {

        activity("main activity", () -> {
            System.out.println("starting main activity");
            activity("sub activity 1", () -> {
                System.out.println("starting sub activity 1");
                sleep("nap time", Duration.ofSeconds(2));
                activity("sub sub activity 1.1", () -> {
                    awaitCondition("wait for counter", () -> execCount.getAndIncrement() > 2, Duration.ofSeconds(3));
                    System.out.println("starting and ending sub sub 1.1");
                });
                activity("sub activity 2", () -> {
                    System.out.println("staring and ending sub 2");
                });
                System.out.println("ending sub activity 1");
            });
            System.out.println("ending main activity");
        });

        return Integer.parseInt(param);

    }
}
