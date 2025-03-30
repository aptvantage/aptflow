package aptvantage.aptflow.examples;

import aptvantage.aptflow.api.RunnableWorkflow;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static aptvantage.aptflow.api.WorkflowFunctions.*;

public class ExampleWorkflowWithAllFunctions implements RunnableWorkflow<Integer, String> {

    private static final AtomicInteger conditionCheckCount = new AtomicInteger(0);
    private final ExampleService service;

    public ExampleWorkflowWithAllFunctions(ExampleService service) {

        this.service = service;
    }

    static void sleepForMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String execute(Integer param) {

        String convertedToString = activity("Convert to String", () -> param.toString());

        boolean okToResume = awaitSignal("OkToResume", Boolean.class);

        System.out.printf("received signal to resume [%s]%n", okToResume);

        activity("Work for between 1 and 5 seconds", service::doSomeWork);

        String concatenated = activity("Concatenate", () -> convertedToString + "asdf");

        sleep("take a nap", Duration.ofSeconds(1));

        CompletableFuture<Void> sleepFor5 = async(()
                -> activity("Work for 5 seconds", () -> sleepForMillis(5_000)));

        CompletableFuture<Void> sleepFor1 = async(()
                -> activity("Work for 1 second", () -> sleepForMillis(1_000)));

        awaitCondition("run a few times", () -> conditionCheckCount.getAndIncrement() > 2, Duration.ofSeconds(3));

        try {
            sleepFor5.get();
            sleepFor1.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        return concatenated;
    }

}
