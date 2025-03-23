package aptvantage.aptflow.examples;

import java.util.Random;

public class ExampleService {

    private final Random random = new Random();

    public void doSomeWork() {
        try {
            Thread.sleep(random.nextLong(5000));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

}
