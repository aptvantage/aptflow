package aptvantage.aptflow.engine.persistence;

import aptvantage.aptflow.model.*;
import org.jdbi.v3.core.Jdbi;

import java.io.Serializable;
import java.util.List;

public class StateReader {

    private final StepFunctionTypeMapper stepFunctionTypeMapper = new StepFunctionTypeMapper();
    private final StepFunctionEventStatusMapper stepFunctionEventStatusMapper = new StepFunctionEventStatusMapper();
    private final InstantColumnMapper instantColumnMapper = new InstantColumnMapper();
    private final SerializableColumnMapper serializableColumnMapper = new SerializableColumnMapper();

    private final Jdbi jdbi;

    public StateReader(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public <I extends Serializable, O extends Serializable> StepFunctionEvent<I, O> getStepFunctionEvent(String id) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT
                                    id,
                                    workflow_run_id,
                                    category,
                                    status,
                                    timestamp
                                FROM event
                                WHERE id = :id
                                """)
                        .bind("id", id)
                        .map((rs, ctx) ->
                                new StepFunctionEvent<I, O>(
                                        rs.getString("id"),
                                        rs.getString("workflow_run_id"),
                                        stepFunctionTypeMapper.map(rs, "category", ctx),
                                        stepFunctionEventStatusMapper.map(rs, "status", ctx),
                                        instantColumnMapper.map(rs, "timestamp", ctx),
                                        this
                                )
                        )
                        .one()
        );
    }

    public <I extends Serializable, O extends Serializable> List<StepFunctionEvent<I, O>> getStepFunctionEventsForWorkflowRun(String workflowRunId) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT
                                    id,
                                    workflow_run_id,
                                    category,
                                    status,
                                    timestamp
                                FROM event
                                WHERE workflow_run_id = :workflowRunId
                                """)
                        .bind("workflowRunId", workflowRunId)
                        .map((rs, ctx) ->
                                new StepFunctionEvent<I, O>(
                                        rs.getString("id"),
                                        rs.getString("workflow_run_id"),
                                        stepFunctionTypeMapper.map(rs, "category", ctx),
                                        stepFunctionEventStatusMapper.map(rs, "status", ctx),
                                        instantColumnMapper.map(rs, "timestamp", ctx),
                                        this
                                )
                        )
                        .collectIntoList()
        );
    }

    public <I extends Serializable, O extends Serializable, A extends Serializable> ActivityFunction<I, O, A>
    getActivityFunction(
            String workflowRunId,
            String name
    ) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT 
                                    workflow_run_id,
                                    name,
                                    started_event_id,
                                    completed_event_id,
                                    output
                                FROM activity
                                WHERE 
                                    workflow_run_id = :workflowRunId
                                    AND name = :name
                                """)
                        .bind("workflowRunId", workflowRunId)
                        .bind("name", name)
                        .map((rs, ctx) ->
                                new ActivityFunction<I, O, A>(
                                        rs.getString("workflow_run_id"),
                                        rs.getString("name"),
                                        rs.getString("started_event_id"),
                                        rs.getString("completed_event_id"),
                                        (A) serializableColumnMapper.map(rs, "output", ctx),
                                        this
                                )
                        )
                        .one()

        );
    }

    public <I extends Serializable, O extends Serializable> List<StepFunction<I, O>> getFunctionsForWorkflowRun(
            String workflowRunId
    ) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT
                                    workflow_run_id,
                                    function_id,
                                    function_type,
                                    started_event_id,
                                    completed_event_id
                                FROM
                                    v_workflow_run_step_function
                                WHERE
                                    workflow_run_id = :workflowRunId
                                """)
                        .bind("workflowRunId", workflowRunId)
                        .map((rs, ctx) -> {
                            StepFunctionType functionType = StepFunctionType.valueOf(rs.getString("function_type"));
                            StepFunction<I, O> stepFunction = switch (functionType) {
                                case WORKFLOW -> throw new IllegalStateException("WORKFLOW is not a StepFunction");
                                case ACTIVITY -> new ActivityFunction<>(
                                        rs.getString("workflow_run_id"),
                                        rs.getString("function_id"),
                                        rs.getString("started_event_id"),
                                        rs.getString("completed_event_id"),
                                        null,
                                        this
                                );
                                case CONDITION -> new ConditionFunction<>(
                                        rs.getString("workflow_run_id"),
                                        rs.getString("function_id"),
                                        rs.getString("started_event_id"),
                                        rs.getString("completed_event_id"),
                                        this
                                );
                                case SIGNAL -> new SignalFunction<>(
                                        rs.getString("workflow_run_id"),
                                        rs.getString("function_id"),
                                        rs.getString("started_event_id"),
                                        rs.getString("completed_event_id"),
                                        null,
                                        this
                                );
                                case SLEEP -> new SleepFunction<>(
                                        rs.getString("workflow_run_id"),
                                        rs.getString("function_id"),
                                        rs.getString("started_event_id"),
                                        rs.getString("completed_event_id"),
                                        null,
                                        this
                                );
                            };
                            return stepFunction;

                        })
                        .collectIntoList()
        );
    }

    public <I extends Serializable, O extends Serializable> Workflow<I, O> getWorkflow(String id) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT id, class_name, input
                                FROM workflow
                                WHERE id = :id
                                """)
                        .bind("id", id)
                        .map((rs, ctx) ->
                                new Workflow<I, O>(
                                        rs.getString("id"),
                                        rs.getString("class_name"),
                                        (I) serializableColumnMapper.map(rs, "input", ctx),
                                        this
                                ))
                        .one()
        );
    }

    public <I extends Serializable, O extends Serializable> WorkflowRun<I,O> getActiveRunForWorkflowId(String workflowId) {
        return (WorkflowRun<I, O>) getRunsForWorkflow(workflowId)
                .stream()
                .filter(run -> run.getArchived() == null)
                .findFirst()
                .orElse(null);
    }

    public <I extends Serializable, O extends Serializable> List<WorkflowRun<I, O>> getRunsForWorkflow(String workflowId) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT 
                                    id,
                                    workflow_id,
                                    scheduled_event_id,
                                    started_event_id,
                                    completed_event_id,
                                    output,
                                    archived
                                FROM
                                    workflow_run
                                WHERE 
                                    workflow_id = :workflowId
                                """)
                        .bind("workflowId", workflowId)
                        .map((rs, ctx) ->
                                new WorkflowRun<I, O>(
                                        rs.getString("id"),
                                        rs.getString("workflow_run_id"),
                                        rs.getString("scheduled_event_id"),
                                        rs.getString("started_event_id"),
                                        rs.getString("completed_event_id"),
                                        (O) serializableColumnMapper.map(rs, "output", ctx),
                                        instantColumnMapper.map(rs, "archived", ctx),
                                        this
                                )
                        )
                        .collectIntoList()
        );
    }

//    private  <I extends Serializable, O extends Serializable> WorkflowRun<I,O> mapResultSet(ResultSet rs, StatementContext ctx) {
//        try {
//            return new WorkflowRun<I, O>(
//                    rs.getString("id"),
//                    rs.getString("workflow_run_id"),
//                    rs.getString("scheduled_event_id"),
//                    rs.getString("started_event_id"),
//                    rs.getString("completed_event_id"),
//                    (O) serializableColumnMapper.map(rs, "output", ctx),
//                    instantColumnMapper.map(rs, "archived", ctx),
//                    workflowRepository
//            );
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }

    public <O extends Serializable, I extends Serializable> WorkflowRun<I, O> getWorkflowRun(String id) {
        return jdbi.withHandle(handle ->
                handle.createQuery("""
                                SELECT 
                                    id,
                                    workflow_id,
                                    scheduled_event_id,
                                    started_event_id,
                                    completed_event_id,
                                    output,
                                    archived
                                FROM
                                    workflow_run
                                WHERE 
                                    id = :id 
                                """)
                        .bind("id", id)
                        .map((rs, ctx) ->
                                new WorkflowRun<I, O>(
                                        rs.getString("id"),
                                        rs.getString("workflow_run_id"),
                                        rs.getString("scheduled_event_id"),
                                        rs.getString("started_event_id"),
                                        rs.getString("completed_event_id"),
                                        (O) serializableColumnMapper.map(rs, "output", ctx),
                                        instantColumnMapper.map(rs, "archived", ctx),
                                        this
                                )
                        )
                        .one()
        );
    }

}
