package aptvantage.aptflow.model;

import java.io.Serializable;
import java.time.Instant;

public record WorkflowRun<I extends Serializable, O extends Serializable>(
        String id,
        Workflow<I> workflow,
        O output,
        Instant created,
        Event scheduled,
        Event started,
        Event completed) {

    public boolean isComplete() {
        return this.completed != null && this.completed.timestamp() != null;
    }
}
