package aptvantage.aptflow.engine;

import aptvantage.aptflow.engine.persistence.WorkflowRepository;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;

public class SignalWorkflowTask extends OneTimeTask<SignalWorkflowTaskInput> {
    private final WorkflowRepository workflowRepository;
    private final WorkflowExecutor workflowExecutor;

    public SignalWorkflowTask(WorkflowRepository workflowRepository,
                              WorkflowExecutor workflowExecutor) {
        super(SignalWorkflowTask.class.getSimpleName(), SignalWorkflowTaskInput.class);
        this.workflowRepository = workflowRepository;
        this.workflowExecutor = workflowExecutor;
    }

    @Override
    public void executeOnce(TaskInstance<SignalWorkflowTaskInput> taskInstance, ExecutionContext executionContext) {
        SignalWorkflowTaskInput data = taskInstance.getData();
        workflowRepository.signalReceived(data.workflowId(), data.signalName(), data.signalValue());
        workflowExecutor.executeWorkflow(data.workflowId());
    }
}
