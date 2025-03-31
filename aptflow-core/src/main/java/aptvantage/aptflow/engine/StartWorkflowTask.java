package aptvantage.aptflow.engine;

import aptvantage.aptflow.engine.persistence.StateWriter;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;

import java.time.Instant;

public class StartWorkflowTask extends OneTimeTask<RunWorkflowTaskInput> {

    private final StateWriter stateWriter;
    private final WorkflowExecutor workflowExecutor;

    public StartWorkflowTask(StateWriter stateWriter,
                             WorkflowExecutor workflowExecutor) {
        super(StartWorkflowTask.class.getSimpleName(), RunWorkflowTaskInput.class);
        //TODO -- need some kind of global error handler for failed tasks
        // they should truly be an edge case (eg, exception handling had an unhandled exception,
        // but we still shouldn't die in silence
        this.stateWriter = stateWriter;
        this.workflowExecutor = workflowExecutor;
    }

    @Override
    public void executeOnce(TaskInstance<RunWorkflowTaskInput> taskInstance, ExecutionContext executionContext) {
        RunWorkflowTaskInput data = taskInstance.getData();
        stateWriter.workflowRunStarted(data.workflowRunId(), Instant.now());
        workflowExecutor.executeWorkflow(data.workflowRunId());
    }
}
