package aptvantage.aptflow;

import aptvantage.aptflow.util.*;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class AcceptanceTest {

    @Container
    private static final PostgreSQLContainer postgresqlContainer = new PostgreSQLContainer()
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
        String workflowId = "simple-workflow-test";

        // when the workflow is submitted
        this.aptWorkflow.executor().runWorkflow(ExampleSimpleWorkflow.class, 777, workflowId);

        // then it eventually completes
        Awaitility.await().atMost(1, TimeUnit.MINUTES)
                .until(() -> this.aptWorkflow.executor().isWorkflowCompleted(workflowId));

        // and we receive the expected output
        String output = this.aptWorkflow.executor().getWorkflowOutput(workflowId, String.class);
        assertEquals("777", output);

        //TODO -- assert on the expected events

    }

    @Nested
    @DisplayName("Activity Tests")
    class ActivityTests {

        @Test
        public void testActivity() throws Exception {
            //given we run a workflow with an activity
            String workflowId = "activity-test";
            aptWorkflow.executor().runWorkflow(ExampleWorkflowWithActivity.class, 777, workflowId);

            // when the workflow completes
            Awaitility.await().atMost(1, TimeUnit.MINUTES)
                    .until(() -> aptWorkflow.executor().isWorkflowCompleted(workflowId));

            // then we receive the expected output
            String output = aptWorkflow.executor().getWorkflowOutput(workflowId, String.class);

            assertEquals("777", output);

            // and we see the expected events
            //TODO assert on expected events
        }

        @Test
        public void testNestedActivities() throws Exception {

            String workflowId = "nested-activity-test";

            aptWorkflow.executor().runWorkflow(ExampleWorkflowWithNestedActivities.class, "900", workflowId);

            Awaitility.await().atMost(1, TimeUnit.MINUTES)
                    .until(() -> aptWorkflow.executor().getWorkflowOutput(workflowId, Integer.class) != null);

            int output = aptWorkflow.executor().getWorkflowOutput(workflowId, Integer.class);
            assertEquals(900, output);
        }

    }


    @Test
    public void test() throws Exception {

        String workflowId = "test-workflow";
        aptWorkflow.executor().runWorkflow(ExampleWorkflow.class, 666, workflowId);

        aptWorkflow.executor().signalWorkflow(workflowId, "OkToResume", true);

        Awaitility.await().atMost(1, TimeUnit.MINUTES)
                .until(() -> "666asdf".equals(aptWorkflow.executor().getWorkflowOutput(workflowId, String.class)));
        System.out.println("done: " + aptWorkflow.executor().getWorkflowOutput(workflowId, String.class));
    }


}
