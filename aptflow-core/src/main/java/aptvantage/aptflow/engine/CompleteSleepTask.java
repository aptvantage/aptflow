package aptvantage.aptflow.engine;

import aptvantage.aptflow.engine.persistence.WorkflowRepository;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;

public class CompleteSleepTask extends OneTimeTask<CompleteSleepTaskInput> {
    private final WorkflowRepository workflowRepository;
    private final WorkflowExecutor workflowExecutor;

    public CompleteSleepTask(WorkflowRepository workflowRepository,
                             WorkflowExecutor workflowExecutor) {
        super(CompleteSleepTask.class.getSimpleName(), CompleteSleepTaskInput.class);
        this.workflowRepository = workflowRepository;
        this.workflowExecutor = workflowExecutor;
    }

    @Override
    public void executeOnce(TaskInstance<CompleteSleepTaskInput> taskInstance, ExecutionContext executionContext) {
        CompleteSleepTaskInput data = taskInstance.getData();
        workflowRepository.sleepCompleted(data.workflowId(), data.sleepIdentifier());
        workflowExecutor.executeWorkflow(data.workflowId());
    }
}
