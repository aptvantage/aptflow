package aptvantage.aptflow.engine;

import aptvantage.aptflow.engine.persistence.StateWriter;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;

import java.time.Instant;

public class CompleteSleepTask extends OneTimeTask<CompleteSleepTaskInput> {
    private final StateWriter stateWriter;
    private final WorkflowExecutor workflowExecutor;

    public CompleteSleepTask(StateWriter stateWriter,
                             WorkflowExecutor workflowExecutor) {
        super(CompleteSleepTask.class.getSimpleName(), CompleteSleepTaskInput.class);
        this.stateWriter = stateWriter;
        this.workflowExecutor = workflowExecutor;
    }

    @Override
    public void executeOnce(TaskInstance<CompleteSleepTaskInput> taskInstance, ExecutionContext executionContext) {
        CompleteSleepTaskInput data = taskInstance.getData();
        stateWriter.sleepCompleted(data.workflowId(), data.sleepIdentifier(), Instant.now());
        workflowExecutor.executeWorkflow(data.workflowId());
    }
}
