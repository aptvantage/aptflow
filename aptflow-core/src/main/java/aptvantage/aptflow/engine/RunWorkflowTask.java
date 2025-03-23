package aptvantage.aptflow.engine;

import aptvantage.aptflow.engine.persistence.WorkflowRepository;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;

public class RunWorkflowTask extends OneTimeTask<StartWorkflowTaskInput> {

    private final WorkflowRepository workflowRepository;
    private final WorkflowExecutor workflowExecutor;

    public RunWorkflowTask(WorkflowRepository workflowRepository,
                           WorkflowExecutor workflowExecutor) {
        super(RunWorkflowTask.class.getSimpleName(), StartWorkflowTaskInput.class);
        //TODO -- need some kind of global error handler for failed tasks
        // they should truly be an edge case (eg, exception handling had an unhandled exception,
        // but we still shouldn't die in silence
        this.workflowRepository = workflowRepository;
        this.workflowExecutor = workflowExecutor;
    }

    @Override
    public void executeOnce(TaskInstance<StartWorkflowTaskInput> taskInstance, ExecutionContext executionContext) {
        StartWorkflowTaskInput data = taskInstance.getData();
        workflowRepository.workflowStarted(data.workflowId());
        workflowExecutor.executeWorkflow(data.workflowId());
    }
}
