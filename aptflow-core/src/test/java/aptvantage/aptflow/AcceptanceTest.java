package aptvantage.aptflow;

import aptvantage.aptflow.api.RunnableWorkflow;
import aptvantage.aptflow.examples.*;
import aptvantage.aptflow.model.*;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class AcceptanceTest {

    @Container
    private static final PostgreSQLContainer postgresqlContainer = new PostgreSQLContainer("postgres")
            .withDatabaseName("test-database")
            .withUsername("test-user")
            .withPassword("test-password");

    static AptWorkflow aptWorkflow;

    @BeforeAll
    static void setup() {
        aptWorkflow = AptWorkflow.builder()
                .dataSource("test-user", "test-password", postgresqlContainer.getJdbcUrl())
                .registerWorkflowDependencies(new ExampleService())
                .start();
    }

    @AfterAll
    static void destroy() {
        aptWorkflow.stop();
    }

    static <I extends Serializable, O extends Serializable> boolean eventMatches(StepFunctionEvent<I, O> event, StepFunctionType category, StepFunctionEventStatus status, String functionId) {
        return eventMatches(event, category, status) && functionId.equals(event.getStepFunction().getId());
    }

    static <I extends Serializable, O extends Serializable> boolean eventMatches(StepFunctionEvent<I, O> event, StepFunctionType category, StepFunctionEventStatus status) {
        return event.getFunctionType() == category && event.getStatus() == status;
    }

    @Test
    @Execution(ExecutionMode.CONCURRENT)
    public void testReRunWorkflow() throws Exception {
        // When a workflow runs
        String workflowId = "testReRunWorkflow";
        Class<? extends RunnableWorkflow<Integer, String>> workflowClass = ExampleSimpleWorkflow.class;
        aptWorkflow.runWorkflow(workflowClass, 888, workflowId);

        // then it will eventually complete
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                aptWorkflow.getLatestRun(workflowId).hasCompleted());

        // and then the output is correct
        WorkflowRun<Integer, String> run1 = aptWorkflow.getLatestRun(workflowId, workflowClass);
        assertEquals("888", run1.getOutput());
        assertEquals("testReRunWorkflow::1", run1.getId());

        // and when we re-run the workflow
        aptWorkflow.reRunWorkflowFromStart(workflowId);

        // then the workflow eventually completes again
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                aptWorkflow.getLatestRun(workflowId).hasCompleted());

        // and then the output is correct again
        WorkflowRun<Integer, String> run2 = aptWorkflow.getLatestRun(workflowId, workflowClass);
        assertEquals("888", run2.getOutput());
        assertEquals("testReRunWorkflow::2", run2.getId());

        // and the expected run history is correct
        Workflow<Integer, String> result = aptWorkflow.getWorkflowResult(workflowId, workflowClass);
        assertEquals(2, result.getWorkflowRuns().size());
        assertEquals("testReRunWorkflow::1", result.getWorkflowRuns().get(0).getId());
        assertEquals("testReRunWorkflow::2", result.getWorkflowRuns().get(1).getId());

    }

    @Test
    @Execution(ExecutionMode.CONCURRENT)
    public void testSimpleWorkflow() throws Exception {
        // given a simple workflow that converts an integer to a string
        String workflowId = "testSimpleWorkflow";
        Class<? extends RunnableWorkflow<Integer, String>> workflowClass = ExampleSimpleWorkflow.class;

        // when the workflow is submitted
        aptWorkflow.runWorkflow(workflowClass, 777, workflowId);

        // then it eventually completes
        Awaitility.await().atMost(1, TimeUnit.MINUTES)
                .until(() -> aptWorkflow.getLatestRun(workflowId).hasCompleted());

        // and the expected output is correct
        String output = aptWorkflow.getLatestRun(workflowId, workflowClass).getOutput();
        assertEquals("777", output);

        List<StepFunctionEvent<Integer, String>> events = aptWorkflow.getLatestRun(workflowId, workflowClass).getFunctionEvents();
        assertEquals(3, events.size());
        assertTrue(eventMatches(events.get(0), StepFunctionType.WORKFLOW, StepFunctionEventStatus.SCHEDULED));
        assertTrue(eventMatches(events.get(1), StepFunctionType.WORKFLOW, StepFunctionEventStatus.STARTED));
        assertTrue(eventMatches(events.get(2), StepFunctionType.WORKFLOW, StepFunctionEventStatus.COMPLETED));

    }

    @Test
    @Execution(ExecutionMode.CONCURRENT)
    public void testWorkflowWithSignal() throws Exception {
        // given a running workflow with a signal has been started
        String workflowId = "testWorkflowWithSignal";
        Class<? extends RunnableWorkflow<Integer, String>> workflowClass = ExampleWorkflowWithSignal.class;
        aptWorkflow.runWorkflow(workflowClass, 777, workflowId);

        // then the workflow will eventually be waiting for a signal
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(
                () -> aptWorkflow.getLatestRun(workflowId).isWaitingForSignal()
        );

        // and when the signal is sent
        aptWorkflow.signalWorkflow(workflowId, "multiplyBy", 10);

        // then the workflow completes
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                aptWorkflow.getLatestRun(workflowId).hasCompleted()
        );

        // and the expected result is received
        assertEquals("7770", aptWorkflow.getLatestRun(workflowId, workflowClass).getOutput());

        // and the event sequence is correct
        List<StepFunctionEvent<Integer, String>> events = aptWorkflow.getLatestRun(workflowId, workflowClass).getFunctionEvents();
        assertEquals(5, events.size());  // 3 WORKFLOW events and 2 SIGNAL events
        assertTrue(eventMatches(events.get(0), StepFunctionType.WORKFLOW, StepFunctionEventStatus.SCHEDULED));
        assertTrue(eventMatches(events.get(1), StepFunctionType.WORKFLOW, StepFunctionEventStatus.STARTED));
        assertTrue(eventMatches(events.get(2), StepFunctionType.SIGNAL, StepFunctionEventStatus.WAITING));
        assertTrue(eventMatches(events.get(3), StepFunctionType.SIGNAL, StepFunctionEventStatus.RECEIVED));
        assertTrue(eventMatches(events.get(4), StepFunctionType.WORKFLOW, StepFunctionEventStatus.COMPLETED));

    }

    @Test
    @Execution(ExecutionMode.CONCURRENT)
    public void testWorkflowWithSleep() throws Exception {
        // given a workflow ran with sleep functions
        String workflowId = "testWorkflowWithSleep";
        Class<? extends RunnableWorkflow<Integer, String>> workflowClass = ExampleWorkflowWithSleep.class;
        aptWorkflow.runWorkflow(workflowClass, 777, workflowId);

        // then the workflow will eventually complete
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                aptWorkflow.getLatestRun(workflowId).hasCompleted());

        // and the output is correct
        String output = aptWorkflow.getLatestRun(workflowId, workflowClass).getOutput();
        assertEquals("777", output);

        // and the event sequence is correct
        List<StepFunctionEvent<Integer, String>> events = aptWorkflow.getLatestRun(workflowId, workflowClass).getFunctionEvents();
        assertEquals(5, events.size());
        assertTrue(eventMatches(events.get(0), StepFunctionType.WORKFLOW, StepFunctionEventStatus.SCHEDULED));
        assertTrue(eventMatches(events.get(1), StepFunctionType.WORKFLOW, StepFunctionEventStatus.STARTED));
        assertTrue(eventMatches(events.get(2), StepFunctionType.SLEEP, StepFunctionEventStatus.STARTED));
        assertTrue(eventMatches(events.get(3), StepFunctionType.SLEEP, StepFunctionEventStatus.COMPLETED));
        assertTrue(eventMatches(events.get(4), StepFunctionType.WORKFLOW, StepFunctionEventStatus.COMPLETED));
    }

    @Test
    @Execution(ExecutionMode.CONCURRENT)
    public void testWorkflowWithCondition() throws Exception {
        // given a workflow run with a condition
        String workflowId = "testWorkflowWithCondition";
        Class<? extends RunnableWorkflow<Integer, String>> workflowClass = ExampleWorkflowWithCondition.class;
        aptWorkflow.runWorkflow(workflowClass, 3, workflowId);

        // then the workflow eventually completes
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                aptWorkflow.getLatestRun(workflowId).hasCompleted());

        // and the output is correct
        String output = aptWorkflow.getLatestRun(workflowId, workflowClass).getOutput();
        assertEquals("3", output);

        // and the event sequence is correct
        List<StepFunctionEvent<Integer, String>> events = aptWorkflow.getLatestRun(workflowId, workflowClass).getFunctionEvents();
        assertEquals(5, events.size());
        assertTrue(eventMatches(events.get(0), StepFunctionType.WORKFLOW, StepFunctionEventStatus.SCHEDULED));
        assertTrue(eventMatches(events.get(1), StepFunctionType.WORKFLOW, StepFunctionEventStatus.STARTED));
        assertTrue(eventMatches(events.get(2), StepFunctionType.CONDITION, StepFunctionEventStatus.WAITING));
        assertTrue(eventMatches(events.get(3), StepFunctionType.CONDITION, StepFunctionEventStatus.SATISFIED));
        assertTrue(eventMatches(events.get(4), StepFunctionType.WORKFLOW, StepFunctionEventStatus.COMPLETED));

    }

    @Test
    @Execution(ExecutionMode.CONCURRENT)
    public void testExampleWorkflowWithAllFunctions() throws Exception {
        // When a workflow with all functions is run
        String workflowId = "testExampleWorkflowWithAllFunctions";
        Class<? extends RunnableWorkflow<Integer, String>> workflowClass = ExampleWorkflowWithAllFunctions.class;
        aptWorkflow.runWorkflow(workflowClass, 666, workflowId);

        // then the workflow will start
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                aptWorkflow.getLatestRun(workflowId).hasStarted());

        // and the workflow will eventually wait for a signal
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                aptWorkflow.getLatestRun(workflowId).isWaitingForSignal());

        // when we signal the workflow
        aptWorkflow.signalWorkflow(workflowId, "OkToResume", true);

        // then the workflow will eventually complete
        Awaitility.await().atMost(1, TimeUnit.MINUTES).until(() ->
                aptWorkflow.getLatestRun(workflowId).hasCompleted()
        );

        // and the output is correct (which also implies successful completion)
        assertEquals("666asdf", aptWorkflow.getLatestRun(workflowId, workflowClass).getOutput());

        // and the event stack contains every function
        List<StepFunctionEvent<Integer, String>> events = aptWorkflow.getLatestRun(workflowId, workflowClass).getFunctionEvents();
        assertTrue(events.stream().anyMatch(event -> event.getFunctionType() == StepFunctionType.ACTIVITY));
        assertTrue(events.stream().anyMatch(event -> event.getFunctionType() == StepFunctionType.SIGNAL));
        assertTrue(events.stream().anyMatch(event -> event.getFunctionType() == StepFunctionType.SLEEP));
        assertTrue(events.stream().anyMatch(event -> event.getFunctionType() == StepFunctionType.WORKFLOW));
        assertTrue(events.stream().anyMatch(event -> event.getFunctionType() == StepFunctionType.CONDITION));
    }

    @Nested
    @DisplayName("Activity Tests")
    class ActivityTests {

        @Test
        @Execution(ExecutionMode.CONCURRENT)
        public void testWorkflowWithActivity() throws Exception {
            //given we run a workflow with an activity
            String workflowId = "testWorkflowWithActivity";
            Class<? extends RunnableWorkflow<Integer, String>> workflowClass = ExampleWorkflowWithActivity.class;
            aptWorkflow.runWorkflow(workflowClass, 777, workflowId);

            // when the workflow completes
            Awaitility.await().atMost(1, TimeUnit.MINUTES)
                    .until(() -> aptWorkflow.getLatestRun(workflowId).hasCompleted());

            // then the output is correct
            String output = aptWorkflow.getLatestRun(workflowId, workflowClass).getOutput();
            assertEquals("777", output);

            // and the event sequence is correct
            List<StepFunctionEvent<Integer, String>> events = aptWorkflow.getLatestRun(workflowId, workflowClass).getFunctionEvents();
            assertEquals(5, events.size()); // 3 workflow and 2 activity
            assertTrue(eventMatches(events.get(0), StepFunctionType.WORKFLOW, StepFunctionEventStatus.SCHEDULED));
            assertTrue(eventMatches(events.get(1), StepFunctionType.WORKFLOW, StepFunctionEventStatus.STARTED));
            assertTrue(eventMatches(events.get(2), StepFunctionType.ACTIVITY, StepFunctionEventStatus.STARTED));
            assertTrue(eventMatches(events.get(3), StepFunctionType.ACTIVITY, StepFunctionEventStatus.COMPLETED));
            assertTrue(eventMatches(events.get(4), StepFunctionType.WORKFLOW, StepFunctionEventStatus.COMPLETED));
        }

        @Test
        @Execution(ExecutionMode.CONCURRENT)
        public void testWorkflowWithAsyncActivities() throws Exception {
            // given we run a workflow with async
            String workflowId = "testWorkflowWithAsyncActivities";
            Class<? extends RunnableWorkflow<Integer, String>> workflowClass = ExampleWorkflowWithAsyncActivities.class;
            aptWorkflow.runWorkflow(workflowClass, 777, workflowId);

            // then it will eventually finish
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                    aptWorkflow.getLatestRun(workflowId).hasCompleted());

            // and the output will be correct
            String output = aptWorkflow.getLatestRun(workflowId, workflowClass).getOutput();
            assertEquals("param: [777] oneSecondEcho: [1-seconds] twoSecondEcho: [2-seconds]", output);

            // and the activities run in parallel (2 activity starts followed by 2 completes)
            List<StepFunctionEvent<Integer, String>> events = aptWorkflow.getLatestRun(workflowId, workflowClass).getFunctionEvents();
            assertEquals(7, events.size());
            assertTrue(eventMatches(events.get(0), StepFunctionType.WORKFLOW, StepFunctionEventStatus.SCHEDULED));
            assertTrue(eventMatches(events.get(1), StepFunctionType.WORKFLOW, StepFunctionEventStatus.STARTED));
            assertTrue(eventMatches(events.get(2), StepFunctionType.ACTIVITY, StepFunctionEventStatus.STARTED));
            assertTrue(eventMatches(events.get(3), StepFunctionType.ACTIVITY, StepFunctionEventStatus.STARTED));
            assertTrue(eventMatches(events.get(4), StepFunctionType.ACTIVITY, StepFunctionEventStatus.COMPLETED));
            assertTrue(eventMatches(events.get(5), StepFunctionType.ACTIVITY, StepFunctionEventStatus.COMPLETED));
            assertTrue(eventMatches(events.get(6), StepFunctionType.WORKFLOW, StepFunctionEventStatus.COMPLETED));
        }

        @Test
        @Execution(ExecutionMode.CONCURRENT)
        public void testWorkflowWithNestedActivities() throws Exception {

            // given we run a workflow with nested activity
            String workflowId = "testWorkflowWithNestedActivities";
            Class<? extends RunnableWorkflow<String, Integer>> workflowClass = ExampleWorkflowWithNestedActivities.class;
            aptWorkflow.runWorkflow(workflowClass, "900", workflowId);

            // then the workflow eventually completes
            Awaitility.await().atMost(1, TimeUnit.MINUTES)
                    .until(() -> aptWorkflow.getLatestRun(workflowId).hasCompleted());

            // and the output is correct
            int output = aptWorkflow.getLatestRun(workflowId, workflowClass).getOutput();
            assertEquals(900, output);

            // and the event sequence is correct
            List<StepFunctionEvent<String, Integer>> events = aptWorkflow.getLatestRun(workflowId, workflowClass).getFunctionEvents();
            assertEquals(15, events.size());
            assertTrue(eventMatches(events.get(0), StepFunctionType.WORKFLOW, StepFunctionEventStatus.SCHEDULED));
            assertTrue(eventMatches(events.get(1), StepFunctionType.WORKFLOW, StepFunctionEventStatus.STARTED));
            assertTrue(eventMatches(events.get(2), StepFunctionType.ACTIVITY, StepFunctionEventStatus.STARTED, "1"));
            assertTrue(eventMatches(events.get(3), StepFunctionType.ACTIVITY, StepFunctionEventStatus.STARTED, "1.1"));
            assertTrue(eventMatches(events.get(4), StepFunctionType.SLEEP, StepFunctionEventStatus.STARTED, "1.1.1"));
            assertTrue(eventMatches(events.get(5), StepFunctionType.SLEEP, StepFunctionEventStatus.COMPLETED, "1.1.1"));
            assertTrue(eventMatches(events.get(6), StepFunctionType.ACTIVITY, StepFunctionEventStatus.STARTED, "1.1.2"));
            assertTrue(eventMatches(events.get(7), StepFunctionType.CONDITION, StepFunctionEventStatus.WAITING, "1.1.2.1"));
            assertTrue(eventMatches(events.get(8), StepFunctionType.CONDITION, StepFunctionEventStatus.SATISFIED, "1.1.2.1"));
            assertTrue(eventMatches(events.get(9), StepFunctionType.ACTIVITY, StepFunctionEventStatus.COMPLETED, "1.1.2"));
            assertTrue(eventMatches(events.get(10), StepFunctionType.ACTIVITY, StepFunctionEventStatus.STARTED, "1.1.3"));
            assertTrue(eventMatches(events.get(11), StepFunctionType.ACTIVITY, StepFunctionEventStatus.COMPLETED, "1.1.3"));
            assertTrue(eventMatches(events.get(12), StepFunctionType.ACTIVITY, StepFunctionEventStatus.COMPLETED, "1.1"));
            assertTrue(eventMatches(events.get(13), StepFunctionType.ACTIVITY, StepFunctionEventStatus.COMPLETED, "1"));
            assertTrue(eventMatches(events.get(14), StepFunctionType.WORKFLOW, StepFunctionEventStatus.COMPLETED));

        }

        @Test
        @Execution(ExecutionMode.CONCURRENT)
        public void testWorkflowWithFailedRunnableActivity() throws Exception {
            // given a workflow that will fail
            String workflowId = "testWorkflowWithFailedRunnableActivity";
            Class<? extends RunnableWorkflow<Integer, String>> workflowClass = ExampleWorkflowWithFailedRunnableActivity.class;
            aptWorkflow.runWorkflow(workflowClass, 13, workflowId);

            // then the workflow will eventually fail
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(
                    () -> aptWorkflow.getLatestRun(workflowId).hasFailed()
            );

            // and the event sequence is correct
            List<StepFunctionEvent<Integer, String>> events = aptWorkflow.getLatestRun(workflowId, workflowClass).getFunctionEvents();
            assertEquals(7, events.size()); // 3 workflow + 2 for successful activity + 2 for failed activity
            assertTrue(eventMatches(events.get(0), StepFunctionType.WORKFLOW, StepFunctionEventStatus.SCHEDULED));
            assertTrue(eventMatches(events.get(1), StepFunctionType.WORKFLOW, StepFunctionEventStatus.STARTED));
            assertTrue(eventMatches(events.get(2), StepFunctionType.ACTIVITY, StepFunctionEventStatus.STARTED));
            assertTrue(eventMatches(events.get(3), StepFunctionType.ACTIVITY, StepFunctionEventStatus.COMPLETED));
            assertTrue(eventMatches(events.get(4), StepFunctionType.ACTIVITY, StepFunctionEventStatus.STARTED));
            assertTrue(eventMatches(events.get(5), StepFunctionType.ACTIVITY, StepFunctionEventStatus.FAILED));
            assertTrue(eventMatches(events.get(6), StepFunctionType.WORKFLOW, StepFunctionEventStatus.FAILED));
        }

        @Test
        @Execution(ExecutionMode.CONCURRENT)
        public void testWorkflowWithFailedSupplierActivity() throws Exception {
            // given a workflow that will fail
            String workflowId = "testWorkflowWithFailedSupplierActivity";
            Class<? extends RunnableWorkflow<Integer, String>> workflowClass = ExampleWorkflowWithFailedSupplierActivity.class;
            aptWorkflow.runWorkflow(workflowClass, 13, workflowId);

            // then the workflow will eventually fail
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(
                    () -> aptWorkflow.getLatestRun(workflowId).hasFailed()
            );

            // and the event sequence is correct
            List<StepFunctionEvent<Integer, String>> events = aptWorkflow.getLatestRun(workflowId, workflowClass).getFunctionEvents();
            assertEquals(5, events.size()); // 3 workflow + 2 for failed activity
            assertTrue(eventMatches(events.get(0), StepFunctionType.WORKFLOW, StepFunctionEventStatus.SCHEDULED));
            assertTrue(eventMatches(events.get(1), StepFunctionType.WORKFLOW, StepFunctionEventStatus.STARTED));
            assertTrue(eventMatches(events.get(2), StepFunctionType.ACTIVITY, StepFunctionEventStatus.STARTED));
            assertTrue(eventMatches(events.get(3), StepFunctionType.ACTIVITY, StepFunctionEventStatus.FAILED));
            assertTrue(eventMatches(events.get(4), StepFunctionType.WORKFLOW, StepFunctionEventStatus.FAILED));
        }

    }


}
