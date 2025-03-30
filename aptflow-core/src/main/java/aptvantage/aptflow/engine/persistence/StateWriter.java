package aptvantage.aptflow.engine.persistence;

import aptvantage.aptflow.api.RunnableWorkflow;
import aptvantage.aptflow.model.StepFunctionEventStatus;
import aptvantage.aptflow.model.StepFunctionType;
import aptvantage.aptflow.model.WorkflowRun;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class StateWriter {

    private final Jdbi jdbi;
    private final StateReader stateReader;

    public StateWriter(
            Jdbi jdbi,
            StateReader stateReader
    ) {
        this.jdbi = jdbi;
        this.stateReader = stateReader;
    }

    private static String newEvent(Handle handle, String workflowRunId, StepFunctionType type, StepFunctionEventStatus status) {
        String eventId = UUID.randomUUID().toString();
        handle.createUpdate("""
                        INSERT INTO event (id, workflow_run_id, category, status, timestamp)
                        VALUES (:id, :workflow_run_id, :category, :status, :timestamp)
                        """)
                .bind("id", eventId)
                .bind("workflow_run_id", workflowRunId)
                .bind("category", type)
                .bind("status", status)
                .bind("timestamp", Instant.now())
                .execute();
        return eventId;
    }

    public void newActivityStarted(String workflowRunId, String name) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowRunId, StepFunctionType.ACTIVITY, StepFunctionEventStatus.STARTED);
            handle.createUpdate("""
                            INSERT INTO activity(workflow_run_id, name, started_event_id)
                            VALUES (:workflowRunId, :name, :eventId)
                            """)
                    .bind("workflowRunId", workflowRunId)
                    .bind("name", name)
                    .bind("eventId", eventId)
                    .execute();
        });
    }

    public void failActivity(String workflowRunId, String name) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowRunId, StepFunctionType.ACTIVITY, StepFunctionEventStatus.FAILED);

            handle.createUpdate("""
                            UPDATE activity
                            SET completed_event_id = :eventId
                            WHERE workflow_run_id = :workflowRunId and name = :name
                            """)
                    .bind("workflowRunId", workflowRunId)
                    .bind("name", name)
                    .bind("eventId", eventId)
                    .execute();

        });
    }

    public void completeActivity(String workflowRunId, String name, Serializable output) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowRunId, StepFunctionType.ACTIVITY, StepFunctionEventStatus.COMPLETED);

            handle.createUpdate("""
                            UPDATE activity
                            SET output = :output,
                                completed_event_id = :eventId
                            WHERE workflow_run_id = :workflowRunId and name = :name
                            """)
                    .bind("workflowRunId", workflowRunId)
                    .bind("name", name)
                    .bind("eventId", eventId)
                    .bind("output", serialize(output))
                    .execute();
        });

    }

    public void newSignalWaiting(String workflowRunId, String name) {
        jdbi.useTransaction(handle -> {

            String eventId = newEvent(handle, workflowRunId, StepFunctionType.SIGNAL, StepFunctionEventStatus.WAITING);

            handle.createUpdate("""
                            INSERT INTO signal(workflow_run_id, name, waiting_event_id)
                            VALUES (:workflowRunId, :name, :eventId)
                            """)
                    .bind("workflowRunId", workflowRunId)
                    .bind("name", name)
                    .bind("eventId", eventId)
                    .execute();
        });
    }

    public void signalReceived(String workflowRunId, String name, Serializable value) {
        jdbi.useTransaction(handle -> {

            String eventId = newEvent(handle, workflowRunId, StepFunctionType.SIGNAL, StepFunctionEventStatus.RECEIVED);

            handle.createUpdate("""
                            UPDATE signal
                            SET value = :value,
                                received_event_id = :eventId
                            WHERE workflow_run_id = :workflowRunId and name = :name
                            """)
                    .bind("workflowRunId", workflowRunId)
                    .bind("name", name)
                    .bind("eventId", eventId)
                    .bind("value", serialize(value))
                    .execute();
        });
    }

    public String scheduleNewRunForExistingWorkflow(String workflowId) {
        WorkflowRun workflowRun = stateReader.getActiveRunForWorkflowId(workflowId, null);

        AtomicReference<String> newRunId = new AtomicReference<>();
        jdbi.useTransaction(handle -> {
            // archive existing run
            handle.createUpdate("""
                            UPDATE workflow_run
                            SET archived = :archived
                            WHERE id = :activeRunId
                            """)
                    .bind("archived", Instant.now())
                    .bind("activeRunId", workflowRun.getId())
                    .execute();

            //schedule new run
            newRunId.set(scheduleWorkflowRun(workflowId, workflowClassFromClassName(workflowRun.getWorkflow().getClassName()), handle));
        });

        return newRunId.get();

    }

    public <I extends Serializable, O extends Serializable>
    String scheduleRunForNewWorkflow(
            String workflowId,
            Class<? extends RunnableWorkflow<I, O>> workflowClass,
            I input) {

        AtomicReference<String> workflowRunId = new AtomicReference<>();
        jdbi.useTransaction(handle -> {

            handle.createUpdate("""
                            INSERT INTO workflow(id, class_name, input)
                            VALUES (:id, :className, :input)
                            """)
                    .bind("id", workflowId)
                    .bind("className", workflowClass.getName())
                    .bind("input", serialize(input))
                    .execute();

            workflowRunId.set(scheduleWorkflowRun(workflowId, workflowClass, handle));
        });
        return workflowRunId.get();

    }

    public void workflowRunStarted(String workflowRunId) {
        jdbi.useTransaction(handle -> {

            String eventId = newEvent(handle, workflowRunId, StepFunctionType.WORKFLOW, StepFunctionEventStatus.STARTED);

            handle.createUpdate("""
                            UPDATE workflow_run
                            SET started_event_id = :eventId
                            WHERE id = :workflowRunId""")
                    .bind("eventId", eventId)
                    .bind("workflowRunId", workflowRunId)
                    .execute();
        });
    }

    public void workflowRunCompleted(String workflowRunId, Object output) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowRunId, StepFunctionType.WORKFLOW, StepFunctionEventStatus.COMPLETED);

            handle.createUpdate("""
                            UPDATE workflow_run
                            SET completed_event_id = :eventId,
                                output = :output
                            WHERE id = :workflowRunId""")
                    .bind("eventId", eventId)
                    .bind("output", serialize(output))
                    .bind("workflowRunId", workflowRunId)
                    .execute();

        });
    }

    public void failWorkflowRun(String workflowRunId) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowRunId, StepFunctionType.WORKFLOW, StepFunctionEventStatus.FAILED);

            handle.createUpdate("""
                            UPDATE workflow_run
                            SET completed_event_id = :eventId
                            WHERE id = :workflowRunId""")
                    .bind("eventId", eventId)
                    .bind("workflowRunId", workflowRunId)
                    .execute();

        });
    }

    public void newConditionWaiting(String workflowRunId, String identifier) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowRunId, StepFunctionType.CONDITION, StepFunctionEventStatus.WAITING);
            handle.createUpdate("""
                            INSERT INTO "condition"(workflow_run_id, identifier, waiting_event_id)
                            VALUES (:workflowRunId, :identifier, :eventId)
                            """)
                    .bind("workflowRunId", workflowRunId)
                    .bind("identifier", identifier)
                    .bind("eventId", eventId)
                    .execute();
        });
    }

    public void conditionSatisfied(String workflowRunId, String identifier) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowRunId, StepFunctionType.CONDITION, StepFunctionEventStatus.SATISFIED);

            handle.createUpdate("""
                            UPDATE "condition"
                            SET satisfied_event_id = :eventId
                            WHERE workflow_run_id = :workflowRunId
                                AND identifier = :identifier""")
                    .bind("eventId", eventId)
                    .bind("identifier", identifier)
                    .bind("workflowRunId", workflowRunId)
                    .execute();

        });
    }

    public void newSleepStarted(String workflowRunId, String identifier, Duration duration) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowRunId, StepFunctionType.SLEEP, StepFunctionEventStatus.STARTED);
            handle.createUpdate("""
                            INSERT INTO sleep(workflow_run_id, identifier, duration_in_millis, started_event_id)
                            VALUES (:workflowRunId, :identifier, :durationInMillis, :eventId)
                            """)
                    .bind("workflowRunId", workflowRunId)
                    .bind("identifier", identifier)
                    .bind("durationInMillis", duration.toMillis())
                    .bind("eventId", eventId)
                    .execute();
        });
    }

    public void sleepCompleted(String workflowRunId, String identifier) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowRunId, StepFunctionType.SLEEP, StepFunctionEventStatus.COMPLETED);

            handle.createUpdate("""
                            UPDATE sleep
                            SET completed_event_id = :eventId
                            WHERE workflow_run_id = :workflowRunId
                                AND identifier = :identifier""")
                    .bind("eventId", eventId)
                    .bind("identifier", identifier)
                    .bind("workflowRunId", workflowRunId)
                    .execute();

        });
    }

    private byte[] serialize(Object obj) {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (ObjectOutputStream objectStream = new ObjectOutputStream(byteStream)) {
            objectStream.writeObject(obj);
        } catch (IOException e) {
            // Handle exception
        }
        return byteStream.toByteArray();
    }

    private <I extends Serializable, O extends Serializable>
    Class<? extends RunnableWorkflow<I, O>> workflowClassFromClassName(String className) {
        try {
            return (Class<? extends RunnableWorkflow<I, O>>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    private <I extends Serializable, O extends Serializable>
    String scheduleWorkflowRun(
            String workflowId,
            Class<? extends RunnableWorkflow<I, O>> workflowClass,
            Handle handle) {

        int existingRunCount = handle.createQuery("""
                        SELECT count(id)
                        FROM workflow_run
                        WHERE workflow_id = :workflowId
                        """)
                .bind("workflowId", workflowId)
                .map((rs, ctx) ->
                        rs.getInt(1)
                )
                .one();

        String workflowRunId = "%s::%s".formatted(workflowId, ++existingRunCount);

        handle.createUpdate("""
                        INSERT INTO workflow_run (id, workflow_id)
                        VALUES (:id, :workflowId)
                        """)
                .bind("id", workflowRunId)
                .bind("workflowId", workflowId)
                .execute();

        String eventId = newEvent(handle, workflowRunId, StepFunctionType.WORKFLOW, StepFunctionEventStatus.SCHEDULED);

        handle.createUpdate("""
                        UPDATE workflow_run
                        SET scheduled_event_id = :eventId
                        WHERE id = :workflowRunId""")
                .bind("eventId", eventId)
                .bind("workflowRunId", workflowRunId)
                .execute();

        return workflowRunId;

    }

}
