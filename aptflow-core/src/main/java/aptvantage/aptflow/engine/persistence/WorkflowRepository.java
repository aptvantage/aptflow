package aptvantage.aptflow.engine.persistence;

import aptvantage.aptflow.api.RunnableWorkflow;
import aptvantage.aptflow.model.*;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.ColumnMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class WorkflowRepository {

    private final Jdbi jdbi;

    private final SerializableColumnMapper serializableColumnMapper = new SerializableColumnMapper();
    private final InstantColumnMapper instantColumnMapper = new InstantColumnMapper();
    private final EventCategoryMapper eventCategoryColumnMapper = new EventCategoryMapper();
    private final EventStatusMapper eventStatusColumnMapper = new EventStatusMapper();

    public WorkflowRepository(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    private static String newEvent(Handle handle, String workflowRunId, EventCategory category, EventStatus status) {
        String eventId = UUID.randomUUID().toString();
        handle.createUpdate("""
                        INSERT INTO event (id, workflow_run_id, category, status, timestamp)
                        VALUES (:id, :workflow_run_id, :category, :status, :timestamp)
                        """)
                .bind("id", eventId)
                .bind("workflow_run_id", workflowRunId)
                .bind("category", category)
                .bind("status", status)
                .bind("timestamp", Instant.now())
                .execute();
        return eventId;
    }

    public void newActivityStarted(String workflowRunId, String name) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowRunId, EventCategory.ACTIVITY, EventStatus.STARTED);
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

    public void failActivity(Activity activity) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, activity.workflowRunId(), EventCategory.ACTIVITY, EventStatus.FAILED);

            handle.createUpdate("""
                            UPDATE activity
                            SET completed_event_id = :eventId
                            WHERE workflow_run_id = :workflowRunId and name = :name
                            """)
                    .bind("workflowRunId", activity.workflowRunId())
                    .bind("name", activity.name())
                    .bind("eventId", eventId)
                    .execute();

        });
    }

    public void completeActivity(String workflowRunId, String name, Serializable output) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowRunId, EventCategory.ACTIVITY, EventStatus.COMPLETED);

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

    public Activity getActivity(String workflowRunId, String name) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT
                                 a.workflow_run_id as a_workflow_run_id,
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
                                WHERE a.workflow_run_id = :workflowRunId and a.name = :name
                                """)
                        .bind("workflowRunId", workflowRunId)
                        .bind("name", name)
                        .map((rs, ctx) ->
                                new Activity(
                                        rs.getString("a_workflow_run_id"),
                                        rs.getString("a_name"),
                                        serializableColumnMapper.map(rs, "a_output", ctx),
                                        new Event(
                                                eventCategoryColumnMapper.map(rs, "started_category", ctx),
                                                eventStatusColumnMapper.map(rs, "started_status", ctx),
                                                rs.getString("a_name"),
                                                instantColumnMapper.map(rs, "started_timestamp", ctx)
                                        ),
                                        new Event(
                                                eventCategoryColumnMapper.map(rs, "completed_category", ctx),
                                                eventStatusColumnMapper.map(rs, "completed_status", ctx),
                                                rs.getString("a_name"),
                                                instantColumnMapper.map(rs, "completed_timestamp", ctx)
                                        )
                                ))
                        .findOne()
                        .orElse(null));
    }

    public void newSignalWaiting(String workflowRunId, String name) {
        jdbi.useTransaction(handle -> {

            String eventId = newEvent(handle, workflowRunId, EventCategory.SIGNAL, EventStatus.WAITING);

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

            String eventId = newEvent(handle, workflowRunId, EventCategory.SIGNAL, EventStatus.RECEIVED);

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

    public boolean isSignalReceived(String workflowRunId, String name) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT count(workflow_run_id)
                                FROM signal
                                WHERE workflow_run_id = :workflowRunId
                                    AND name = :name
                                    AND received_event_id IS NOT NULL
                                """)
                        .bind("workflowRunId", workflowRunId)
                        .bind("name", name)
                        .mapTo(Integer.class).one()) == 1;
    }

    public Signal getSignal(String workflowRunId, String name) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT
                                 s.workflow_run_id as s_workflow_run_id,
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
                                WHERE s.workflow_run_id = :workflowRunId and s.name = :name
                                """)
                        .bind("workflowRunId", workflowRunId)
                        .bind("name", name)
                        .map((rs, ctx) ->
                                new Signal(
                                        rs.getString("s_workflow_run_id"),
                                        rs.getString("s_name"),
                                        serializableColumnMapper.map(rs, "s_value", ctx),
                                        new Event(
                                                eventCategoryColumnMapper.map(rs, "waiting_category", ctx),
                                                eventStatusColumnMapper.map(rs, "waiting_status", ctx),
                                                rs.getString("s_name"),
                                                instantColumnMapper.map(rs, "waiting_timestamp", ctx)
                                        ),
                                        new Event(
                                                eventCategoryColumnMapper.map(rs, "received_category", ctx),
                                                eventStatusColumnMapper.map(rs, "received_status", ctx),
                                                rs.getString("s_name"),
                                                instantColumnMapper.map(rs, "received_timestamp", ctx)
                                        )
                                ))
                        .findOne()
                        .orElse(null));
    }

    public String getActiveWorkflowRunId(String workflowId) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT id
                                FROM workflow_run
                                WHERE workflow_id = :workflowId
                                    AND archived IS NULL
                                """)
                        .bind("workflowId", workflowId)
                        .map((rs, ctx) ->
                                rs.getString("id")
                        ).one()
        );
    }

    public WorkflowRunStatus getWorkflowRunStatus(String workflowRunId) {
        EventStatus workflowStatus = getLatestEventStatus(workflowRunId, EventCategory.WORKFLOW);
        List<Function> functions = getFunctions(workflowRunId);
        //TODO -- provide failed functions
        return new WorkflowRunStatus(workflowStatus, functions);

    }

    List<Function> getFunctions(String workflowRunId) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT
                                    function_id,
                                    category,
                                    started,
                                    completed
                                FROM
                                    v_workflow_run_function
                                WHERE
                                    workflow_run_id = :workflowRunId
                                ORDER BY started
                                """)
                        .bind("workflowRunId", workflowRunId)
                        .map((rs, ctx) ->
                                new Function(
                                        rs.getString("function_id"),
                                        eventCategoryColumnMapper.map(rs, "category", ctx),
                                        instantColumnMapper.map(rs, "started", ctx),
                                        instantColumnMapper.map(rs, "completed", ctx)
                                ))
                        .collectIntoList()
        );
    }

    EventStatus getLatestEventStatus(String workflowRunId, EventCategory category) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT status
                                FROM event
                                WHERE workflow_run_id = :workflowRunId
                                    AND category = :category
                                ORDER BY timestamp DESC
                                LIMIT 1
                                """)
                        .bind("workflowRunId", workflowRunId)
                        .bind("category", category)
                        .map((rs, ctx) ->
                                EventStatus.valueOf(rs.getString("status")))
                        .findOne()
                        .orElse(null)
        );
    }

    public List<Event> getWorkflowEvents(String workflowRunId) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT
                                    id,
                                    workflow_run_id,
                                    category,
                                    status,
                                    timestamp,
                                    function_id
                                FROM v_workflow_run_event
                                WHERE workflow_run_id = :workflowRunId
                                ORDER BY timestamp
                                """)
                        .bind("workflowRunId", workflowRunId)
                        .map((rs, ctx) ->
                                new Event(
                                        eventCategoryColumnMapper.map(rs, "category", ctx),
                                        eventStatusColumnMapper.map(rs, "status", ctx),
                                        rs.getString("function_id"),
                                        instantColumnMapper.map(rs, "timestamp", ctx)
                                ))
                        .collectIntoList()
        );
    }

    public <O extends Serializable> WorkflowRun<? extends Serializable, O>
    getWorkflowRun(String workflowRunId, Class<? extends RunnableWorkflow<O, ? extends Serializable>> workflowClass) {
        return getWorkflowRun(workflowRunId);
    }

    public <I extends Serializable, O extends Serializable> WorkflowRun<I, O> getWorkflowRun(String workflowRunId) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT
                                    r.id as r_id,
                                    w.id as w_id,
                                    w.class_name as w_class_name,
                                    w.input as w_input,
                                    r.output as r_output,
                                    r.created as r_created,
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
                                FROM workflow_run r
                                    INNER JOIN workflow w
                                        ON r.workflow_id = w.id
                                  LEFT JOIN event scheduled
                                    ON r.scheduled_event_id = scheduled.id
                                  LEFT JOIN event started
                                    ON r.started_event_id = started.id
                                  LEFT JOIN event completed
                                    ON r.completed_event_id = completed.id
                                WHERE r.id = :id
                                """)
                        .bind("id", workflowRunId)
                        .map((rs, ctx) ->
                                new WorkflowRun<>(
                                        rs.getString("r_id"),
                                        new Workflow<>(
                                                rs.getString("w_id"),
                                                rs.getString("w_class_name"),
                                                (I) serializableColumnMapper.map(rs, "w_input", ctx),
                                                instantColumnMapper.map(rs, "w_created", ctx)
                                        ),
                                        (O) serializableColumnMapper.map(rs, "r_output", ctx),
                                        instantColumnMapper.map(rs, "r_created", ctx),
                                        new Event(
                                                eventCategoryColumnMapper.map(rs, "scheduled_category", ctx),
                                                eventStatusColumnMapper.map(rs, "scheduled_status", ctx),
                                                rs.getString("r_id"),
                                                instantColumnMapper.map(rs, "scheduled_timestamp", ctx)
                                        ),
                                        new Event(
                                                eventCategoryColumnMapper.map(rs, "started_category", ctx),
                                                eventStatusColumnMapper.map(rs, "started_status", ctx),
                                                rs.getString("r_id"),
                                                instantColumnMapper.map(rs, "started_timestamp", ctx)
                                        ),
                                        new Event(
                                                eventCategoryColumnMapper.map(rs, "completed_category", ctx),
                                                eventStatusColumnMapper.map(rs, "completed_status", ctx),
                                                rs.getString("r_id"),
                                                instantColumnMapper.map(rs, "completed_timestamp", ctx)
                                        )
                                ))
                        .findOne()
                        .orElse(null));
    }

    public String scheduleRunForNewWorkflow(String workflowId, Class workflowClass, Object input) {
        String workflowRunId = UUID.randomUUID().toString();
        jdbi.useTransaction(handle -> {

            handle.createUpdate("""
                            INSERT INTO workflow(id, class_name, input)
                            VALUES (:id, :className, :input)
                            """)
                    .bind("id", workflowId)
                    .bind("className", workflowClass.getName())
                    .bind("input", serialize(input))
                    .execute();

            handle.createUpdate("""
                            INSERT INTO workflow_run (id, workflow_id)
                            VALUES (:id, :workflowId)
                            """)
                    .bind("id", workflowRunId)
                    .bind("workflowId", workflowId)
                    .execute();

            String eventId = newEvent(handle, workflowRunId, EventCategory.WORKFLOW, EventStatus.SCHEDULED);

            handle.createUpdate("""
                            UPDATE workflow_run
                            SET scheduled_event_id = :eventId
                            WHERE id = :workflowRunId""")
                    .bind("eventId", eventId)
                    .bind("workflowRunId", workflowRunId)
                    .execute();
        });
        return workflowRunId;

    }

    public void workflowRunStarted(String workflowRunId) {
        jdbi.useTransaction(handle -> {

            String eventId = newEvent(handle, workflowRunId, EventCategory.WORKFLOW, EventStatus.STARTED);

            handle.createUpdate("""
                            UPDATE workflow_run
                            SET started_event_id = :eventId
                            WHERE id = :workflowRunId""")
                    .bind("eventId", eventId)
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

    public void workflowRunCompleted(String workflowRunId, Object output) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowRunId, EventCategory.WORKFLOW, EventStatus.COMPLETED);

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
            String eventId = newEvent(handle, workflowRunId, EventCategory.WORKFLOW, EventStatus.FAILED);

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
            String eventId = newEvent(handle, workflowRunId, EventCategory.CONDITION, EventStatus.WAITING);
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
            String eventId = newEvent(handle, workflowRunId, EventCategory.CONDITION, EventStatus.SATISFIED);

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

    public Condition getCondition(String workflowRunId, String identifier) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT
                                    c.workflow_run_id AS c_workflow_run_id,
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
                                WHERE c.workflow_run_id = :workflowRunId
                                    AND c.identifier = :identifier
                                """)
                        .bind("workflowRunId", workflowRunId)
                        .bind("identifier", identifier)
                        .map((rs, ctx) ->
                                new Condition(
                                        rs.getString("c_workflow_run_id"),
                                        rs.getString("c_identifier"),
                                        new Event(
                                                eventCategoryColumnMapper.map(rs, "waiting_category", ctx),
                                                eventStatusColumnMapper.map(rs, "waiting_status", ctx),
                                                rs.getString("c_identifier"),
                                                instantColumnMapper.map(rs, "waiting_timestamp", ctx)
                                        ),
                                        new Event(
                                                eventCategoryColumnMapper.map(rs, "satisfied_category", ctx),
                                                eventStatusColumnMapper.map(rs, "satisfied_status", ctx),
                                                rs.getString("c_identifier"),
                                                instantColumnMapper.map(rs, "satisfied_timestamp", ctx)
                                        )
                                ))
                        .findOne()
                        .orElse(null));
    }

    public void newSleepStarted(String workflowRunId, String identifier, Duration duration) {
        jdbi.useTransaction(handle -> {
            String eventId = newEvent(handle, workflowRunId, EventCategory.SLEEP, EventStatus.STARTED);
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
            String eventId = newEvent(handle, workflowRunId, EventCategory.SLEEP, EventStatus.COMPLETED);

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

    public Sleep getSleep(String workflowRunId, String identifier) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT
                                    s.workflow_run_id AS s_workflow_run_id,
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
                                WHERE s.workflow_run_id = :workflowRunId
                                    AND s.identifier = :identifier
                                """)
                        .bind("workflowRunId", workflowRunId)
                        .bind("identifier", identifier)
                        .map((rs, ctx) ->
                                new Sleep(
                                        rs.getString("s_workflow_run_id"),
                                        rs.getString("s_identifier"),
                                        Duration.ofMillis(rs.getLong("s_duration_in_millis")),
                                        new Event(
                                                eventCategoryColumnMapper.map(rs, "started_category", ctx),
                                                eventStatusColumnMapper.map(rs, "started_status", ctx),
                                                rs.getString("s_identifier"),
                                                instantColumnMapper.map(rs, "started_timestamp", ctx)
                                        ),
                                        new Event(
                                                eventCategoryColumnMapper.map(rs, "completed_category", ctx),
                                                eventStatusColumnMapper.map(rs, "completed_status", ctx),
                                                rs.getString("s_identifier"),
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

    static class EventCategoryMapper implements ColumnMapper<EventCategory> {

        @Override
        public EventCategory map(ResultSet r, int columnNumber, StatementContext ctx) throws SQLException {
            String stringValue = r.getString(columnNumber);
            if (stringValue == null) {
                return null;
            }
            return EventCategory.valueOf(stringValue);
        }
    }

    public static class EventStatusMapper implements ColumnMapper<EventStatus> {

        @Override
        public EventStatus map(ResultSet r, int columnNumber, StatementContext ctx) throws SQLException {
            String stringValue = r.getString(columnNumber);
            if (stringValue == null) {
                return null;
            }
            return EventStatus.valueOf(stringValue);
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
