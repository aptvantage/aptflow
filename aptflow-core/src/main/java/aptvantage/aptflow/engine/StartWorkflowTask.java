package aptvantage.aptflow.engine;

import aptvantage.aptflow.engine.persistence.WorkflowRepository;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;

public class StartWorkflowTask extends OneTimeTask<RunWorkflowTaskInput> {

    private final WorkflowRepository workflowRepository;
    private final WorkflowExecutor workflowExecutor;

    public StartWorkflowTask(WorkflowRepository workflowRepository,
                             WorkflowExecutor workflowExecutor) {
        super(StartWorkflowTask.class.getSimpleName(), RunWorkflowTaskInput.class);
        //TODO -- need some kind of global error handler for failed tasks
        // they should truly be an edge case (eg, exception handling had an unhandled exception,
        // but we still shouldn't die in silence
        this.workflowRepository = workflowRepository;
        this.workflowExecutor = workflowExecutor;
    }

    @Override
    public void executeOnce(TaskInstance<RunWorkflowTaskInput> taskInstance, ExecutionContext executionContext) {
        RunWorkflowTaskInput data = taskInstance.getData();
        workflowRepository.workflowStarted(data.workflowId());
        workflowExecutor.executeWorkflow(data.workflowId());
    }
}
