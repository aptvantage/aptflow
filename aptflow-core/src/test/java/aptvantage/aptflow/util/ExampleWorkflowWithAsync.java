package aptvantage.aptflow.util;

import aptvantage.aptflow.api.RunnableWorkflow;
import aptvantage.aptflow.api.WorkflowFunctions;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;

public class ExampleWorkflowWithAsync implements RunnableWorkflow<String, Integer> {
    @Override
    public String execute(Integer param) {

        CompletableFuture<String> twoSecondFuture = WorkflowFunctions.async(() ->
                workForDurationAndEcho(Duration.of(2, ChronoUnit.SECONDS), "2-seconds")
        );

        CompletableFuture<String> oneSecondFuture = WorkflowFunctions.async(() ->
                workForDurationAndEcho(Duration.of(1, ChronoUnit.SECONDS), "1-seconds")
        );

        try {
            String oneSecondEcho = oneSecondFuture.get();
            String twoSecondEcho = twoSecondFuture.get();

            return "param: [%s] oneSecondEcho: [%s] twoSecondEcho: [%s]".formatted(param, oneSecondEcho, twoSecondEcho);
        } catch (Exception e) {
            throw new RuntimeException(e.toString(), e);
        }
    }

    public String workForDurationAndEcho(Duration duration, String echo) {
       try {
           Thread.sleep(duration);
           return echo;
       } catch (InterruptedException e) {
           throw new RuntimeException(e);
       }
    }
}
