package aptvantage.aptflow.engine;

import aptvantage.aptflow.engine.persistence.StateWriter;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;

public class SignalWorkflowTask extends OneTimeTask<SignalWorkflowTaskInput> {
    private final StateWriter stateWriter;
    private final WorkflowExecutor workflowExecutor;

    public SignalWorkflowTask(StateWriter stateWriter,
                              WorkflowExecutor workflowExecutor) {
        super(SignalWorkflowTask.class.getSimpleName(), SignalWorkflowTaskInput.class);
        this.stateWriter = stateWriter;
        this.workflowExecutor = workflowExecutor;
    }

    @Override
    public void executeOnce(TaskInstance<SignalWorkflowTaskInput> taskInstance, ExecutionContext executionContext) {
        SignalWorkflowTaskInput data = taskInstance.getData();
        stateWriter.signalReceived(data.workflowId(), data.signalName(), data.signalValue());
        workflowExecutor.executeWorkflow(data.workflowId());
    }
}
