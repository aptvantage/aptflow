package aptvantage.aptflow.engine.persistence;

import aptvantage.aptflow.model.*;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.ColumnMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.core.statement.Update;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

public class WorkflowRepository {

    private final Jdbi jdbi;

    private final SerializableColumnMapper serializableColumnMapper = new SerializableColumnMapper();
    private final InstantColumnMapper instantColumnMapper = new InstantColumnMapper();

    public WorkflowRepository(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    private static String newEvent(Handle handle, String workflowId, EventCategory category, EventStatus status) {
        String eventId = UUID.randomUUID().toString();
        handle.createUpdate("""
                        INSERT INTO event (id, workflow_id, category, status, timestamp)
                        VALUES (:id, :workflow_id, :category, :status, :timestamp)
                        """)
                .bind("id", eventId)
                .bind("workflow_id", workflowId)
                .bind("category", category)
                .bind("status", status)
                .bind("timestamp", Instant.now())
                .execute();
        return eventId;
    }

    public void newActivityStarted(String workflowId, String name) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowId, EventCategory.ACTIVITY, EventStatus.STARTED);
            handle.createUpdate("""
                            INSERT INTO activity(workflow_id, name, started_event_id)
                            VALUES (:workflowId, :name, :eventId)
                            """)
                    .bind("workflowId", workflowId)
                    .bind("name", name)
                    .bind("eventId", eventId)
                    .execute();
        });
    }

    public void completeActivity(String workflowId, String name, Serializable output) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowId, EventCategory.ACTIVITY, EventStatus.COMPLETED);

            handle.createUpdate("""
                            UPDATE activity
                            SET output = :output,
                                completed_event_id = :eventId
                            WHERE workflow_id = :workflowId and name = :name                            
                            """)
                    .bind("workflowId", workflowId)
                    .bind("name", name)
                    .bind("eventId", eventId)
                    .bind("output", serialize(output))
                    .execute();
        });

    }

    public boolean isActivityComplete(String workflowId, String name) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT count(workflow_id)
                                FROM activity
                                WHERE workflow_id = :workflowId
                                    AND name = :name
                                    AND completed_event_id IS NOT NULL
                                """)
                        .bind("workflowId", workflowId)
                        .bind("name", name)
                        .mapTo(Integer.class).one()) == 1;
    }

    public Activity getActivity(String workflowId, String name) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT
                                 a.workflow_id as a_workflow_id,
                                 a.name as a_name,
                                 a.output as a_output,
                                 started.category as started_category,
                                 started.status as started_status,
                                 started.timestamp as started_timestamp,
                                 completed.category as completed_category,
                                 completed.status as completed_status,
                                 completed.timestamp as completed_timestamp
                                FROM activity a
                                  LEFT JOIN event started on a.started_event_id = started.id
                                  LEFT JOIN event completed on a.completed_event_id = completed.id
                                WHERE a.workflow_id = :workflowId and a.name = :name
                                """)
                        .bind("workflowId", workflowId)
                        .bind("name", name)
                        .map((rs, ctx) ->
                                new Activity(
                                        rs.getString("a_workflow_id"),
                                        rs.getString("a_name"),
                                        serializableColumnMapper.map(rs, "a_output", ctx),
                                        new Event(
                                                rs.getString("started_category"),
                                                rs.getString("started_status"),
                                                instantColumnMapper.map(rs, "started_timestamp", ctx)
                                        ),
                                        new Event(
                                                rs.getString("completed_category"),
                                                rs.getString("completed_status"),
                                                instantColumnMapper.map(rs, "completed_timestamp", ctx)
                                        )
                                ))
                        .findOne()
                        .orElse(null));
    }

    public void newSignalWaiting(String workflowId, String name) {
        jdbi.useTransaction(handle -> {

            String eventId = newEvent(handle, workflowId, EventCategory.SIGNAL, EventStatus.WAITING);

            handle.createUpdate("""
                            INSERT INTO signal(workflow_id, name, waiting_event_id)
                            VALUES (:workflowId, :name, :eventId)
                            """)
                    .bind("workflowId", workflowId)
                    .bind("name", name)
                    .bind("eventId", eventId)
                    .execute();
        });
    }

    public void signalReceived(String workflowId, String name, Serializable value) {
        jdbi.useTransaction(handle -> {

            String eventId = newEvent(handle, workflowId, EventCategory.SIGNAL, EventStatus.RECEIVED);

            handle.createUpdate("""
                            UPDATE signal
                            SET value = :value,
                                received_event_id = :eventId
                            WHERE workflow_id = :workflowId and name = :name                            
                            """)
                    .bind("workflowId", workflowId)
                    .bind("name", name)
                    .bind("eventId", eventId)
                    .bind("value", serialize(value))
                    .execute();
        });
    }

    public boolean isSignalReceived(String workflowId, String name) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT count(workflow_id)
                                FROM signal
                                WHERE workflow_id = :workflowId
                                    AND name = :name
                                    AND received_event_id IS NOT NULL
                                """)
                        .bind("workflowId", workflowId)
                        .bind("name", name)
                        .mapTo(Integer.class).one()) == 1;
    }

    public Signal getSignal(String workflowId, String name) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT
                                 s.workflow_id as s_workflow_id,
                                 s.name as s_name,
                                 s.value as s_value,
                                 waiting.category as waiting_category,
                                 waiting.status as waiting_status,
                                 waiting.timestamp as waiting_timestamp,
                                 received.category as received_category,
                                 received.status as received_status,
                                 received.timestamp as received_timestamp
                                FROM signal s
                                  LEFT JOIN event waiting on s.waiting_event_id = waiting.id
                                  LEFT JOIN event received on s.received_event_id = received.id
                                WHERE s.workflow_id = :workflowId and s.name = :name
                                """)
                        .bind("workflowId", workflowId)
                        .bind("name", name)
                        .map((rs, ctx) ->
                                new Signal(
                                        rs.getString("s_workflow_id"),
                                        rs.getString("s_name"),
                                        serializableColumnMapper.map(rs, "s_value", ctx),
                                        new Event(
                                                rs.getString("waiting_category"),
                                                rs.getString("waiting_status"),
                                                instantColumnMapper.map(rs, "waiting_timestamp", ctx)
                                        ),
                                        new Event(
                                                rs.getString("received_category"),
                                                rs.getString("received_status"),
                                                instantColumnMapper.map(rs, "received_timestamp", ctx)
                                        )
                                ))
                        .findOne()
                        .orElse(null));
    }

    public Workflow getWorkflow(String workflowId) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT 
                                    w.id as w_id,
                                    w.class_name as w_class_name,
                                    w.input as w_input,
                                    w.output as w_output,
                                    w.created as w_created,
                                    scheduled.timestamp as scheduled_timestamp,
                                    scheduled.category as scheduled_category,
                                    scheduled.status as scheduled_status,
                                    started.timestamp as started_timestamp,
                                    started.category as started_category,
                                    started.status as started_status,
                                    completed.timestamp as completed_timestamp,
                                    completed.category as completed_category,
                                    completed.status as completed_status
                                FROM workflow w
                                  LEFT JOIN event scheduled
                                    ON w.scheduled_event_id = scheduled.id
                                  LEFT JOIN event started
                                    ON w.started_event_id = started.id
                                  LEFT JOIN event completed
                                    ON w.completed_event_id = completed.id
                                WHERE w.id = :id
                                """)
                        .bind("id", workflowId)
                        .map((rs, ctx) ->
                                new Workflow(
                                        rs.getString("w_id"),
                                        rs.getString("w_class_name"),
                                        serializableColumnMapper.map(rs, "w_input", ctx),
                                        serializableColumnMapper.map(rs, "w_output", ctx),
                                        instantColumnMapper.map(rs, "w_created", ctx),
                                        new Event(
                                                rs.getString("scheduled_category"),
                                                rs.getString("scheduled_status"),
                                                instantColumnMapper.map(rs, "scheduled_timestamp", ctx)
                                        ),
                                        new Event(
                                                rs.getString("started_category"),
                                                rs.getString("started_status"),
                                                instantColumnMapper.map(rs, "started_timestamp", ctx)
                                        ),
                                        new Event(
                                                rs.getString("completed_category"),
                                                rs.getString("completed_status"),
                                                instantColumnMapper.map(rs, "completed_timestamp", ctx)
                                        )
                                ))
                        .findOne()
                        .orElse(null));
    }

    public void newWorkflowScheduled(String workflowId, Class workflowClass, Object input) {
        jdbi.useTransaction(handle -> {
            Update update = handle.createUpdate("""
                    INSERT INTO workflow (id, class_name, input, created)
                    VALUES (:id, :class_name, :input, :created)
                    """);
            update.bind("id", workflowId);
            update.bind("class_name", workflowClass.getName());
            update.bind("created", Instant.now());
            update.bind("input", serialize(input));
            update.execute();

            String eventId = newEvent(handle, workflowId, EventCategory.WORKFLOW, EventStatus.SCHEDULED);

            handle.createUpdate("""
                            UPDATE workflow
                            SET scheduled_event_id = :eventId
                            WHERE id = :workflowId""")
                    .bind("eventId", eventId)
                    .bind("workflowId", workflowId)
                    .execute();
        });

    }

    public boolean hasWorkflowStarted(String workflowId) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT count(id)
                                FROM event
                                WHERE workflow_id = :workflow_id
                                    AND category = :category
                                    AND status = :status
                                """)
                        .bind("workflow_id", workflowId)
                        .bind("category", EventCategory.WORKFLOW)
                        .bind("status", EventStatus.STARTED)
                        .mapTo(Integer.class).one()) == 1;
    }

    public void workflowStarted(String workflowId) {
        jdbi.useTransaction(handle -> {

            String eventId = newEvent(handle, workflowId, EventCategory.WORKFLOW, EventStatus.STARTED);

            handle.createUpdate("""
                            UPDATE workflow
                            SET started_event_id = :eventId
                            WHERE id = :workflowId""")
                    .bind("eventId", eventId)
                    .bind("workflowId", workflowId)
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

    public void workflowCompleted(String workflowId, Object output) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowId, EventCategory.WORKFLOW, EventStatus.COMPLETED);

            handle.createUpdate("""
                            UPDATE workflow
                            SET completed_event_id = :eventId,
                                output = :output
                            WHERE id = :workflowId""")
                    .bind("eventId", eventId)
                    .bind("output", serialize(output))
                    .bind("workflowId", workflowId)
                    .execute();

        });
    }

    public void newConditionWaiting(String workflowId, String identifier) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowId, EventCategory.CONDITION, EventStatus.WAITING);
            handle.createUpdate("""
                            INSERT INTO "condition"(workflow_id, identifier, waiting_event_id)
                            VALUES (:workflowId, :identifier, :eventId)
                            """)
                    .bind("workflowId", workflowId)
                    .bind("identifier", identifier)
                    .bind("eventId", eventId)
                    .execute();
        });
    }

    public void conditionSatisfied(String workflowId, String identifier) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowId, EventCategory.CONDITION, EventStatus.SATISFIED);

            handle.createUpdate("""
                            UPDATE "condition"
                            SET satisfied_event_id = :eventId
                            WHERE workflow_id = :workflowId
                                AND identifier = :identifier""")
                    .bind("eventId", eventId)
                    .bind("identifier", identifier)
                    .bind("workflowId", workflowId)
                    .execute();

        });
    }

    public Condition getCondition(String workflowId, String identifier) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT 
                                    c.workflow_id AS c_workflow_id,
                                    c.identifier AS c_identifier,                                                                 
                                    waiting.timestamp AS waiting_timestamp,
                                    waiting.category AS waiting_category,
                                    waiting.status AS waiting_status,
                                    satisfied.timestamp AS satisfied_timestamp,
                                    satisfied.category AS satisfied_category,
                                    satisfied.status AS satisfied_status
                                FROM "condition" c
                                  LEFT JOIN event waiting
                                    ON c.waiting_event_id = waiting.id
                                  LEFT JOIN event satisfied
                                    ON c.satisfied_event_id = satisfied.id
                                WHERE c.workflow_id = :workflowId 
                                    AND c.identifier = :identifier
                                """)
                        .bind("workflowId", workflowId)
                        .bind("identifier", identifier)
                        .map((rs, ctx) ->
                                new Condition(
                                        rs.getString("c_workflow_id"),
                                        rs.getString("c_identifier"),
                                        new Event(
                                                rs.getString("waiting_category"),
                                                rs.getString("waiting_status"),
                                                instantColumnMapper.map(rs, "waiting_timestamp", ctx)
                                        ),
                                        new Event(
                                                rs.getString("satisfied_category"),
                                                rs.getString("satisfied_status"),
                                                instantColumnMapper.map(rs, "satisfied_timestamp", ctx)
                                        )
                                ))
                        .findOne()
                        .orElse(null));
    }

    public void newSleepStarted(String workflowId, String identifier, Duration duration) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowId, EventCategory.SLEEP, EventStatus.STARTED);
            handle.createUpdate("""
                            INSERT INTO sleep(workflow_id, identifier, duration_in_millis, started_event_id)
                            VALUES (:workflowId, :identifier, :durationInMillis, :eventId)
                            """)
                    .bind("workflowId", workflowId)
                    .bind("identifier", identifier)
                    .bind("durationInMillis", duration.toMillis())
                    .bind("eventId", eventId)
                    .execute();
        });
    }

    public void sleepCompleted(String workflowId, String identifier) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowId, EventCategory.SLEEP, EventStatus.COMPLETED);

            handle.createUpdate("""
                            UPDATE sleep
                            SET completed_event_id = :eventId
                            WHERE workflow_id = :workflowId
                                AND identifier = :identifier""")
                    .bind("eventId", eventId)
                    .bind("identifier", identifier)
                    .bind("workflowId", workflowId)
                    .execute();

        });
    }

    public Sleep getSleep(String workflowId, String identifier) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT 
                                    s.workflow_id AS s_workflow_id,
                                    s.identifier AS s_identifier,
                                    s.duration_in_millis AS s_duration_in_millis,                                    
                                    started.timestamp AS started_timestamp,
                                    started.category AS started_category,
                                    started.status AS started_status,
                                    completed.timestamp AS completed_timestamp,
                                    completed.category AS completed_category,
                                    completed.status AS completed_status
                                FROM sleep s
                                  LEFT JOIN event started
                                    ON s.started_event_id = started.id
                                  LEFT JOIN event completed
                                    ON s.completed_event_id = completed.id
                                WHERE s.workflow_id = :workflowId 
                                    AND s.identifier = :identifier
                                """)
                        .bind("workflowId", workflowId)
                        .bind("identifier", identifier)
                        .map((rs, ctx) ->
                                new Sleep(
                                        rs.getString("s_workflow_id"),
                                        rs.getString("s_identifier"),
                                        Duration.ofMillis(rs.getLong("s_duration_in_millis")),
                                        new Event(
                                                rs.getString("started_category"),
                                                rs.getString("started_status"),
                                                instantColumnMapper.map(rs, "started_timestamp", ctx)
                                        ),
                                        new Event(
                                                rs.getString("completed_category"),
                                                rs.getString("completed_status"),
                                                instantColumnMapper.map(rs, "completed_timestamp", ctx)
                                        )
                                ))
                        .findOne()
                        .orElse(null));
    }

    static class SerializableColumnMapper implements ColumnMapper<Serializable> {

        @Override
        public Serializable map(ResultSet rs, int columnNumber, StatementContext ctx) throws SQLException {
            byte[] bytes = rs.getBytes(columnNumber);
            if (bytes == null) {
                return null;
            }
            try (ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
                 ObjectInputStream objectIn = new ObjectInputStream(byteIn)) {
                return (Serializable) objectIn.readObject();
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class InstantColumnMapper implements ColumnMapper<Instant> {
        public Instant map(ResultSet rs, int columnNumber, StatementContext ctx) throws SQLException {
            Timestamp timestamp = rs.getTimestamp(columnNumber);
            if (timestamp == null) {
                return null;
            }
            return Instant.ofEpochMilli(timestamp.getTime());
        }
    }

}
