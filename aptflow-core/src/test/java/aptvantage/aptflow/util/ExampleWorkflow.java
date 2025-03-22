package aptvantage.aptflow.util;

import aptvantage.aptflow.api.RunnableWorkflow;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static aptvantage.aptflow.api.WorkflowFunctions.*;

public class ExampleWorkflow implements RunnableWorkflow<String, Integer> {

    private final ExampleService service;

    private static AtomicInteger conditionCheckCount = new AtomicInteger(0);

    public ExampleWorkflow(ExampleService service) {

        this.service = service;
    }

    @Override
    public String execute(Integer param) {

        String convertedToString = activity("Convert to String", () -> param.toString());

        boolean okToResume = awaitSignal("OkToResume", Boolean.class);

        System.out.println("received signal to resume [%s]".formatted(okToResume));

        activity("Work for between 1 and 5 seconds", service::doSomeWork);

        String concatenated = activity("Concatenate", () -> convertedToString + "asdf");

        sleep("take a nap", Duration.ofSeconds(1));

        CompletableFuture<Void> sleepFor10 = async(()
                -> activity("Work for 10 seconds", () -> sleepForMillis(10_000)));

        CompletableFuture<Void> sleepFor1 = async(()
                -> activity("Work for 1 second", () -> sleepForMillis(1_000)));

        awaitCondition("run a few times", () -> conditionCheckCount.getAndIncrement() > 2, Duration.ofSeconds(3));

        try {
            sleepFor10.get();
            sleepFor1.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        return concatenated;
    }

    static void sleepForMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
