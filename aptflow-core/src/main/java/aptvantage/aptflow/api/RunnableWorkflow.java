package aptvantage.aptflow.api;

public interface RunnableWorkflow<R, P> {

    R execute(P param);
}
