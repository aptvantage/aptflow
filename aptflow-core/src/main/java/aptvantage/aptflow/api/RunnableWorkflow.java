package aptvantage.aptflow.api;

import java.io.Serializable;

public interface RunnableWorkflow<I extends Serializable, O extends Serializable> {
    O execute(I param);
}
