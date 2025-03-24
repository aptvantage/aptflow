package aptvantage.aptflow.model;

import org.jdbi.v3.core.mapper.reflect.ColumnName;

import java.io.Serializable;
import java.time.Instant;

// TODO - let's see if we can genericise these inputs and outputs a little better
public record Workflow<INPUT extends Serializable, OUTPUT extends Serializable>(
        String id,
        @ColumnName("class_name") String className,
        INPUT input,
        OUTPUT output,
        Instant created,
        Event scheduled,
        Event started,
        Event completed) {

    public boolean isComplete() {
        return this.completed != null && this.completed.timestamp() != null;
    }
}
