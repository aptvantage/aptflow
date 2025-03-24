package aptvantage.aptflow.api;

import java.io.Serializable;

public interface RunnableWorkflow<OUTPUT extends Serializable, INPUT extends Serializable> {
    OUTPUT execute(INPUT param);
}
