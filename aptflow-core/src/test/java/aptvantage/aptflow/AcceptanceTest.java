package aptvantage.aptflow;

import aptvantage.aptflow.examples.*;
import aptvantage.aptflow.model.Event;
import aptvantage.aptflow.model.EventCategory;
import aptvantage.aptflow.model.EventStatus;
import aptvantage.aptflow.model.Function;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
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

    AptWorkflow aptWorkflow;

    @BeforeEach
    void setup() {
        this.aptWorkflow = AptWorkflow.builder()
                .dataSource("test-user", "test-password", postgresqlContainer.getJdbcUrl())
                .registerWorkflowDependencies(new ExampleService())
                .start();
    }

    @AfterEach
    void destroy() {
        this.aptWorkflow.stop();
    }

    @Test
    public void testSimpleWorkflow() throws Exception {
        // given a simple workflow that converts an integer to a string
        String workflowId = "testSimpleWorkflow";

        // when the workflow is submitted
        this.aptWorkflow.runWorkflow(ExampleSimpleWorkflow.class, 777, workflowId);

        // then it eventually completes
        Awaitility.await().atMost(1, TimeUnit.MINUTES)
                .until(() -> this.aptWorkflow.isWorkflowCompleted(workflowId));

        // and we receive the expected output
        String output = this.aptWorkflow.getWorkflowOutput(workflowId, String.class);
        assertEquals("777", output);

        List<Event> events = this.aptWorkflow.getWorkflowEvents(workflowId);
        assertEquals(3, events.size());
        assertTrue(eventMatches(events.get(0), EventCategory.WORKFLOW, EventStatus.SCHEDULED));
        assertTrue(eventMatches(events.get(1), EventCategory.WORKFLOW, EventStatus.STARTED));
        assertTrue(eventMatches(events.get(2), EventCategory.WORKFLOW, EventStatus.COMPLETED));

    }

    @Test
    public void testWorkflowWithSignal() throws Exception {
        // given a running workflow with a signal has been started
        String workflowId = "testWorkflowWithSignal";
        this.aptWorkflow.runWorkflow(ExampleWorkflowWithSignal.class, 777, workflowId);

        // then the status will be waiting for a signal
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
                    List<Function> activeFunctions = this.aptWorkflow.getWorkflowStatus(workflowId).activeFunctions();
                    return !activeFunctions.isEmpty() && activeFunctions.get(0).category() == EventCategory.SIGNAL;
                }
        );

        // and when we send the signal
        this.aptWorkflow.signalWorkflow(workflowId, "multiplyBy", 10);

        // then the workflow completes
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                this.aptWorkflow.getWorkflowStatus(workflowId).isComplete()
        );

        // and we get the expected result
        assertEquals("7770", this.aptWorkflow.getWorkflowOutput(workflowId, String.class));

        // and we have the correct events
        List<Event> events = this.aptWorkflow.getWorkflowEvents(workflowId);
        assertEquals(5, events.size());  // 3 WORKFLOW events and 2 SIGNAL events
        assertTrue(eventMatches(events.get(0), EventCategory.WORKFLOW, EventStatus.SCHEDULED));
        assertTrue(eventMatches(events.get(1), EventCategory.WORKFLOW, EventStatus.STARTED));
        assertTrue(eventMatches(events.get(2), EventCategory.SIGNAL, EventStatus.WAITING));
        assertTrue(eventMatches(events.get(3), EventCategory.SIGNAL, EventStatus.RECEIVED));
        assertTrue(eventMatches(events.get(4), EventCategory.WORKFLOW, EventStatus.COMPLETED));

    }

    @Test
    public void testWorkflowWithAsyncActivities() throws Exception {
        // given we run a workflow with async
        String workflowId = "testWorkflowWithAsyncActivities";
        this.aptWorkflow.runWorkflow(ExampleWorkflowWithAsyncActivities.class, 777, workflowId);

        // then it will eventually finish
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                this.aptWorkflow.getWorkflowStatus(workflowId).isComplete());

        // and we will get the expected output
        String output = this.aptWorkflow.getWorkflowOutput(workflowId, String.class);
        assertEquals("param: [777] oneSecondEcho: [1-seconds] twoSecondEcho: [2-seconds]", output);

        // and we get the expected event list
        List<Event> events = this.aptWorkflow.getWorkflowEvents(workflowId);
        assertEquals(7, events.size());
        assertTrue(eventMatches(events.get(0), EventCategory.WORKFLOW, EventStatus.SCHEDULED));
        assertTrue(eventMatches(events.get(1), EventCategory.WORKFLOW, EventStatus.STARTED));
        // and the activities run in parallel
        assertTrue(eventMatches(events.get(2), EventCategory.ACTIVITY, EventStatus.STARTED, "2-seconds"));
        assertTrue(eventMatches(events.get(3), EventCategory.ACTIVITY, EventStatus.STARTED, "1-seconds"));
        assertTrue(eventMatches(events.get(4), EventCategory.ACTIVITY, EventStatus.COMPLETED, "1-seconds"));
        assertTrue(eventMatches(events.get(5), EventCategory.ACTIVITY, EventStatus.COMPLETED, "2-seconds"));
        assertTrue(eventMatches(events.get(6), EventCategory.WORKFLOW, EventStatus.COMPLETED));
    }

    boolean eventMatches(Event event, EventCategory category, EventStatus status, String functionId) {
        return eventMatches(event, category, status) && functionId.equals(event.functionId());
    }

    boolean eventMatches(Event event, EventCategory category, EventStatus status) {
        return event.category() == category && event.status() == status;
    }

    @Test
    public void test() throws Exception {

        String workflowId = "test-workflow";
        aptWorkflow.runWorkflow(ExampleWorkflow.class, 666, workflowId);

        aptWorkflow.signalWorkflow(workflowId, "OkToResume", true);

        Awaitility.await().atMost(1, TimeUnit.MINUTES)
                .until(() -> "666asdf".equals(aptWorkflow.getWorkflowOutput(workflowId, String.class)));
        System.out.println("done: " + aptWorkflow.getWorkflowOutput(workflowId, String.class));
    }

    @Nested
    @DisplayName("Activity Tests")
    class ActivityTests {

        @Test
        public void testWorkflowWithActivity() throws Exception {
            //given we run a workflow with an activity
            String workflowId = "testWorkflowWithActivity";
            aptWorkflow.runWorkflow(ExampleWorkflowWithActivity.class, 777, workflowId);

            // when the workflow completes
            Awaitility.await().atMost(1, TimeUnit.MINUTES)
                    .until(() -> aptWorkflow.isWorkflowCompleted(workflowId));

            // then we receive the expected output
            String output = aptWorkflow.getWorkflowOutput(workflowId, String.class);
            assertEquals("777", output);

            // and we see the expected events
            List<Event> events = aptWorkflow.getWorkflowEvents(workflowId);
            assertEquals(5, events.size()); // 3 workflow and 2 activity
            assertTrue(eventMatches(events.get(0), EventCategory.WORKFLOW, EventStatus.SCHEDULED));
            assertTrue(eventMatches(events.get(1), EventCategory.WORKFLOW, EventStatus.STARTED));
            assertTrue(eventMatches(events.get(2), EventCategory.ACTIVITY, EventStatus.STARTED));
            assertTrue(eventMatches(events.get(3), EventCategory.ACTIVITY, EventStatus.COMPLETED));
            assertTrue(eventMatches(events.get(4), EventCategory.WORKFLOW, EventStatus.COMPLETED));
        }

        @Test
        public void testNestedActivities() throws Exception {

            // given we run a workflow with nested activity
            String workflowId = "workflowWithNestedActivities";
            aptWorkflow.runWorkflow(ExampleWorkflowWithNestedActivities.class, "900", workflowId);

            // when the workflow is complete
            Awaitility.await().atMost(1, TimeUnit.MINUTES)
                    .until(() -> aptWorkflow.getWorkflowOutput(workflowId, Integer.class) != null);

            // then we get the expected output
            int output = aptWorkflow.getWorkflowOutput(workflowId, Integer.class);
            assertEquals(900, output);

            // and we get the expected event sequence
            List<Event> events = aptWorkflow.getWorkflowEvents(workflowId);
            assertEquals(15, events.size());
            assertTrue(eventMatches(events.get(0), EventCategory.WORKFLOW, EventStatus.SCHEDULED, "workflowWithNestedActivities"));
            assertTrue(eventMatches(events.get(1), EventCategory.WORKFLOW, EventStatus.STARTED, "workflowWithNestedActivities"));
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
            assertTrue(eventMatches(events.get(14), EventCategory.WORKFLOW, EventStatus.COMPLETED, "workflowWithNestedActivities"));

        }

    }


}
