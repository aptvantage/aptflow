package aptvantage.aptflow.engine;

import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;

public class ResumeStartedWorkflowTask extends OneTimeTask<RunWorkflowTaskInput> {

    private final WorkflowExecutor workflowExecutor;

    public ResumeStartedWorkflowTask(
            WorkflowExecutor workflowExecutor) {
        super(ResumeStartedWorkflowTask.class.getSimpleName(), RunWorkflowTaskInput.class);
        //TODO -- need some kind of global error handler for failed tasks
        // they should truly be an edge case (eg, exception handling had an unhandled exception,
        // but we still shouldn't die in silence
        this.workflowExecutor = workflowExecutor;
    }

    @Override
    public void executeOnce(TaskInstance<RunWorkflowTaskInput> taskInstance, ExecutionContext executionContext) {
        RunWorkflowTaskInput data = taskInstance.getData();
        workflowExecutor.executeWorkflow(data.workflowRunId());
    }
}
