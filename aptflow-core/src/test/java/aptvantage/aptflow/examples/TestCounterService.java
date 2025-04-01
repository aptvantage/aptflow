package aptvantage.aptflow.examples;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TestCounterService {

    private final Map<String, Integer> testCountMap = new ConcurrentHashMap<>();

    public int incrementAndGetTestCount(String testName) {
        int incrementedCount = testCountMap.computeIfAbsent(testName, k -> 0) + 1;
        testCountMap.put(testName, incrementedCount);
        return incrementedCount;
    }

    public int getTestCount(String testName) {
        return testCountMap.computeIfAbsent(testName, k -> 0);
    }

}
