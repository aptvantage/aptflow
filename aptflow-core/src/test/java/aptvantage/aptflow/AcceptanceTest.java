package aptvantage.aptflow;

import aptvantage.aptflow.examples.*;
import aptvantage.aptflow.model.v1.Event;
import aptvantage.aptflow.model.v1.EventCategory;
import aptvantage.aptflow.model.v1.EventStatus;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

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

    static boolean eventMatches(Event event, EventCategory category, EventStatus status, String functionId) {
        return eventMatches(event, category, status) && functionId.equals(event.functionId());
    }

    static boolean eventMatches(Event event, EventCategory category, EventStatus status) {
        return event.category() == category && event.status() == status;
    }

    @Test
    @Execution(ExecutionMode.CONCURRENT)
    public void testReRunWorkflow() throws Exception {
        // When a workflow runs
        String workflowId = "testReRunWorkflow";
        aptWorkflow.runWorkflow(ExampleSimpleWorkflow.class,888, workflowId);

        // then it will eventually complete
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                aptWorkflow.getWorkflowStatus(workflowId).isComplete());

        // and then the output is correct
        String output = aptWorkflow.getWorkflowOutput(workflowId, ExampleSimpleWorkflow.class);
        assertEquals("888", output);

        // and when we re-run the workflow
        aptWorkflow.reRunWorkflowFromStart(workflowId);

        // then the workflow eventually completes again
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                aptWorkflow.getWorkflowStatus(workflowId).isComplete());

        // and then the output is correct again
        output = aptWorkflow.getWorkflowOutput(workflowId, ExampleSimpleWorkflow.class);
        assertEquals("888", output);

        // and the expected run history is correct
        //TODO run history

    }

    @Test
    @Execution(ExecutionMode.CONCURRENT)
    public void testSimpleWorkflow() throws Exception {
        // given a simple workflow that converts an integer to a string
        String workflowId = "testSimpleWorkflow";

        // when the workflow is submitted
        aptWorkflow.runWorkflow(ExampleSimpleWorkflow.class, 777, workflowId);

        // then it eventually completes
        Awaitility.await().atMost(1, TimeUnit.MINUTES)
                .until(() -> aptWorkflow.getWorkflowStatus(workflowId).isComplete());

        // and the expected output is correct
        String output = aptWorkflow.getWorkflowOutput(workflowId, ExampleSimpleWorkflow.class);
        assertEquals("777", output);

        List<Event> events = aptWorkflow.getWorkflowEvents(workflowId);
        assertEquals(3, events.size());
        assertTrue(eventMatches(events.get(0), EventCategory.WORKFLOW, EventStatus.SCHEDULED));
        assertTrue(eventMatches(events.get(1), EventCategory.WORKFLOW, EventStatus.STARTED));
        assertTrue(eventMatches(events.get(2), EventCategory.WORKFLOW, EventStatus.COMPLETED));

    }

    @Test
    @Execution(ExecutionMode.CONCURRENT)
    public void testWorkflowWithSignal() throws Exception {
        // given a running workflow with a signal has been started
        String workflowId = "testWorkflowWithSignal";
        aptWorkflow.runWorkflow(ExampleWorkflowWithSignal.class, 777, workflowId);

        // then the workflow will eventually be waiting for a signal
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(
                () -> aptWorkflow.getWorkflowStatus(workflowId).isWaitingForSignal()
        );

        // and when the signal is sent
        aptWorkflow.signalWorkflow(workflowId, "multiplyBy", 10);

        // then the workflow completes
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                aptWorkflow.getWorkflowStatus(workflowId).isComplete()
        );

        // and the expected result is received
        assertEquals("7770", aptWorkflow.getWorkflowOutput(workflowId, ExampleWorkflowWithSignal.class));

        // and the event sequence is correct
        List<Event> events = aptWorkflow.getWorkflowEvents(workflowId);
        assertEquals(5, events.size());  // 3 WORKFLOW events and 2 SIGNAL events
        assertTrue(eventMatches(events.get(0), EventCategory.WORKFLOW, EventStatus.SCHEDULED));
        assertTrue(eventMatches(events.get(1), EventCategory.WORKFLOW, EventStatus.STARTED));
        assertTrue(eventMatches(events.get(2), EventCategory.SIGNAL, EventStatus.WAITING));
        assertTrue(eventMatches(events.get(3), EventCategory.SIGNAL, EventStatus.RECEIVED));
        assertTrue(eventMatches(events.get(4), EventCategory.WORKFLOW, EventStatus.COMPLETED));

    }

    @Test
    @Execution(ExecutionMode.CONCURRENT)
    public void testWorkflowWithSleep() throws Exception {
        // given a workflow ran with sleep functions
        String workflowId = "testWorkflowWithSleep";
        aptWorkflow.runWorkflow(ExampleWorkflowWithSleep.class, 777, workflowId);

        // then the workflow will eventually complete
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                aptWorkflow.getWorkflowStatus(workflowId).isComplete());

        // and the output is correct
        String output = aptWorkflow.getWorkflowOutput(workflowId, ExampleWorkflowWithSleep.class);
        assertEquals("777", output);

        // and the event sequence is correct
        List<Event> events = aptWorkflow.getWorkflowEvents(workflowId);
        assertEquals(5, events.size());
        assertTrue(eventMatches(events.get(0), EventCategory.WORKFLOW, EventStatus.SCHEDULED));
        assertTrue(eventMatches(events.get(1), EventCategory.WORKFLOW, EventStatus.STARTED));
        assertTrue(eventMatches(events.get(2), EventCategory.SLEEP, EventStatus.STARTED));
        assertTrue(eventMatches(events.get(3), EventCategory.SLEEP, EventStatus.COMPLETED));
        assertTrue(eventMatches(events.get(4), EventCategory.WORKFLOW, EventStatus.COMPLETED));
    }

    @Test
    @Execution(ExecutionMode.CONCURRENT)
    public void testWorkflowWithCondition() throws Exception {
        // given a workflow run with a condition
        String workflowId = "testWorkflowWithCondition";
        aptWorkflow.runWorkflow(ExampleWorkflowWithCondition.class, 3, workflowId);

        // then the workflow eventually completes
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                aptWorkflow.getWorkflowStatus(workflowId).isComplete());

        // and the output is correct
        String output = aptWorkflow.getWorkflowOutput(workflowId, ExampleWorkflowWithCondition.class);
        assertEquals("3", output);

        // and the event sequence is correct
        List<Event> events = aptWorkflow.getWorkflowEvents(workflowId);
        assertEquals(5, events.size());
        assertTrue(eventMatches(events.get(0), EventCategory.WORKFLOW, EventStatus.SCHEDULED));
        assertTrue(eventMatches(events.get(1), EventCategory.WORKFLOW, EventStatus.STARTED));
        assertTrue(eventMatches(events.get(2), EventCategory.CONDITION, EventStatus.WAITING));
        assertTrue(eventMatches(events.get(3), EventCategory.CONDITION, EventStatus.SATISFIED));
        assertTrue(eventMatches(events.get(4), EventCategory.WORKFLOW, EventStatus.COMPLETED));

    }

    @Test
    @Execution(ExecutionMode.CONCURRENT)
    public void testExampleWorkflowWithAllFunctions() throws Exception {
        // When a workflow with all functions is run
        String workflowId = "testExampleWorkflowWithAllFunctions";
        aptWorkflow.runWorkflow(ExampleWorkflowWithAllFunctions.class, 666, workflowId);

        // then the workflow will start
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                aptWorkflow.getWorkflowStatus(workflowId).hasStarted());

        // and the workflow will eventually wait for a signal
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                aptWorkflow.getWorkflowStatus(workflowId).isWaitingForSignal());

        // when we signal the workflow
        aptWorkflow.signalWorkflow(workflowId, "OkToResume", true);

        // then the workflow will eventually complete
        Awaitility.await().atMost(1, TimeUnit.MINUTES).until(() ->
                aptWorkflow.getWorkflowStatus(workflowId).isComplete()
        );

        // and the output is correct (which also implies successful completion)
        assertEquals("666asdf", aptWorkflow.getWorkflowOutput(workflowId, ExampleWorkflowWithAllFunctions.class));

        // and the event stack contains every function
        List<Event> events = aptWorkflow.getWorkflowEvents(workflowId);
        assertTrue(events.stream().anyMatch(event -> event.category() == EventCategory.ACTIVITY));
        assertTrue(events.stream().anyMatch(event -> event.category() == EventCategory.SIGNAL));
        assertTrue(events.stream().anyMatch(event -> event.category() == EventCategory.SLEEP));
        assertTrue(events.stream().anyMatch(event -> event.category() == EventCategory.WORKFLOW));
        assertTrue(events.stream().anyMatch(event -> event.category() == EventCategory.CONDITION));
    }

    @Nested
    @DisplayName("Activity Tests")
    class ActivityTests {

        @Test
        @Execution(ExecutionMode.CONCURRENT)
        public void testWorkflowWithActivity() throws Exception {
            //given we run a workflow with an activity
            String workflowId = "testWorkflowWithActivity";
            aptWorkflow.runWorkflow(ExampleWorkflowWithActivity.class, 777, workflowId);

            // when the workflow completes
            Awaitility.await().atMost(1, TimeUnit.MINUTES)
                    .until(() -> aptWorkflow.getWorkflowStatus(workflowId).isComplete());

            // then the output is correct
            String output = aptWorkflow.getWorkflowOutput(workflowId, ExampleWorkflowWithActivity.class);
            assertEquals("777", output);

            // and the event sequence is correct
            List<Event> events = aptWorkflow.getWorkflowEvents(workflowId);
            assertEquals(5, events.size()); // 3 workflow and 2 activity
            assertTrue(eventMatches(events.get(0), EventCategory.WORKFLOW, EventStatus.SCHEDULED));
            assertTrue(eventMatches(events.get(1), EventCategory.WORKFLOW, EventStatus.STARTED));
            assertTrue(eventMatches(events.get(2), EventCategory.ACTIVITY, EventStatus.STARTED));
            assertTrue(eventMatches(events.get(3), EventCategory.ACTIVITY, EventStatus.COMPLETED));
            assertTrue(eventMatches(events.get(4), EventCategory.WORKFLOW, EventStatus.COMPLETED));
        }

        @Test
        @Execution(ExecutionMode.CONCURRENT)
        public void testWorkflowWithAsyncActivities() throws Exception {
            // given we run a workflow with async
            String workflowId = "testWorkflowWithAsyncActivities";
            aptWorkflow.runWorkflow(ExampleWorkflowWithAsyncActivities.class, 777, workflowId);

            // then it will eventually finish
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                    aptWorkflow.getWorkflowStatus(workflowId).isComplete());

            // and the output will be correct
            String output = aptWorkflow.getWorkflowOutput(workflowId, ExampleWorkflowWithAsyncActivities.class);
            assertEquals("param: [777] oneSecondEcho: [1-seconds] twoSecondEcho: [2-seconds]", output);

            // and the activities run in parallel (2 activity starts followed by 2 completes)
            List<Event> events = aptWorkflow.getWorkflowEvents(workflowId);
            assertEquals(7, events.size());
            assertTrue(eventMatches(events.get(0), EventCategory.WORKFLOW, EventStatus.SCHEDULED));
            assertTrue(eventMatches(events.get(1), EventCategory.WORKFLOW, EventStatus.STARTED));
            assertTrue(eventMatches(events.get(2), EventCategory.ACTIVITY, EventStatus.STARTED));
            assertTrue(eventMatches(events.get(3), EventCategory.ACTIVITY, EventStatus.STARTED));
            assertTrue(eventMatches(events.get(4), EventCategory.ACTIVITY, EventStatus.COMPLETED));
            assertTrue(eventMatches(events.get(5), EventCategory.ACTIVITY, EventStatus.COMPLETED));
            assertTrue(eventMatches(events.get(6), EventCategory.WORKFLOW, EventStatus.COMPLETED));
        }

        @Test
        @Execution(ExecutionMode.CONCURRENT)
        public void testWorkflowWithNestedActivities() throws Exception {

            // given we run a workflow with nested activity
            String workflowId = "testWorkflowWithNestedActivities";
            aptWorkflow.runWorkflow(ExampleWorkflowWithNestedActivities.class, "900", workflowId);

            // then the workflow eventually completes
            Awaitility.await().atMost(1, TimeUnit.MINUTES)
                    .until(() -> aptWorkflow.getWorkflowOutput(workflowId, ExampleWorkflowWithNestedActivities.class) != null);

            // and the output is correct
            int output = aptWorkflow.getWorkflowOutput(workflowId, ExampleWorkflowWithNestedActivities.class);
            assertEquals(900, output);

            // and the event sequence is correct
            List<Event> events = aptWorkflow.getWorkflowEvents(workflowId);
            assertEquals(15, events.size());
            assertTrue(eventMatches(events.get(0), EventCategory.WORKFLOW, EventStatus.SCHEDULED));
            assertTrue(eventMatches(events.get(1), EventCategory.WORKFLOW, EventStatus.STARTED));
            assertTrue(eventMatches(events.get(2), EventCategory.ACTIVITY, EventStatus.STARTED, "1"));
            assertTrue(eventMatches(events.get(3), EventCategory.ACTIVITY, EventStatus.STARTED, "1.1"));
            assertTrue(eventMatches(events.get(4), EventCategory.SLEEP, EventStatus.STARTED, "1.1.1"));
            assertTrue(eventMatches(events.get(5), EventCategory.SLEEP, EventStatus.COMPLETED, "1.1.1"));
            assertTrue(eventMatches(events.get(6), EventCategory.ACTIVITY, EventStatus.STARTED, "1.1.2"));
            assertTrue(eventMatches(events.get(7), EventCategory.CONDITION, EventStatus.WAITING, "1.1.2.1"));
            assertTrue(eventMatches(events.get(8), EventCategory.CONDITION, EventStatus.SATISFIED, "1.1.2.1"));
            assertTrue(eventMatches(events.get(9), EventCategory.ACTIVITY, EventStatus.COMPLETED, "1.1.2"));
            assertTrue(eventMatches(events.get(10), EventCategory.ACTIVITY, EventStatus.STARTED, "1.1.3"));
            assertTrue(eventMatches(events.get(11), EventCategory.ACTIVITY, EventStatus.COMPLETED, "1.1.3"));
            assertTrue(eventMatches(events.get(12), EventCategory.ACTIVITY, EventStatus.COMPLETED, "1.1"));
            assertTrue(eventMatches(events.get(13), EventCategory.ACTIVITY, EventStatus.COMPLETED, "1"));
            assertTrue(eventMatches(events.get(14), EventCategory.WORKFLOW, EventStatus.COMPLETED));

        }

        @Test
        @Execution(ExecutionMode.CONCURRENT)
        public void testWorkflowWithFailedRunnableActivity() throws Exception {
            // given a workflow that will fail
            String workflowId = "testWorkflowWithFailedRunnableActivity";
            aptWorkflow.runWorkflow(ExampleWorkflowWithFailedRunnableActivity.class, 13, workflowId);

            // then the workflow will eventually fail
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(
                    () -> aptWorkflow.getWorkflowStatus(workflowId).hasFailed()
            );

            // and the event sequence is correct
            List<Event> events = aptWorkflow.getWorkflowEvents(workflowId);
            assertEquals(7, events.size()); // 3 workflow + 2 for successful activity + 2 for failed activity
            assertTrue(eventMatches(events.get(0), EventCategory.WORKFLOW, EventStatus.SCHEDULED));
            assertTrue(eventMatches(events.get(1), EventCategory.WORKFLOW, EventStatus.STARTED));
            assertTrue(eventMatches(events.get(2), EventCategory.ACTIVITY, EventStatus.STARTED));
            assertTrue(eventMatches(events.get(3), EventCategory.ACTIVITY, EventStatus.COMPLETED));
            assertTrue(eventMatches(events.get(4), EventCategory.ACTIVITY, EventStatus.STARTED));
            assertTrue(eventMatches(events.get(5), EventCategory.ACTIVITY, EventStatus.FAILED));
            assertTrue(eventMatches(events.get(6), EventCategory.WORKFLOW, EventStatus.FAILED));
        }

        @Test
        @Execution(ExecutionMode.CONCURRENT)
        public void testWorkflowWithFailedSupplierActivity() throws Exception {
            // given a workflow that will fail
            String workflowId = "testWorkflowWithFailedSupplierActivity";
            aptWorkflow.runWorkflow(ExampleWorkflowWithFailedSupplierActivity.class, 13, workflowId);

            // then the workflow will eventually fail
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(
                    () -> aptWorkflow.getWorkflowStatus(workflowId).hasFailed()
            );

            // and the event sequence is correct
            List<Event> events = aptWorkflow.getWorkflowEvents(workflowId);
            assertEquals(5, events.size()); // 3 workflow + 2 for failed activity
            assertTrue(eventMatches(events.get(0), EventCategory.WORKFLOW, EventStatus.SCHEDULED));
            assertTrue(eventMatches(events.get(1), EventCategory.WORKFLOW, EventStatus.STARTED));
            assertTrue(eventMatches(events.get(2), EventCategory.ACTIVITY, EventStatus.STARTED));
            assertTrue(eventMatches(events.get(3), EventCategory.ACTIVITY, EventStatus.FAILED));
            assertTrue(eventMatches(events.get(4), EventCategory.WORKFLOW, EventStatus.FAILED));
        }

    }


}
