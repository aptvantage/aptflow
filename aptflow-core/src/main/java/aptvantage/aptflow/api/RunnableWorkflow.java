package aptvantage.aptflow.api;

public interface RunnableWorkflow<R, P> {
    // TODO - R & P should be Serializable
    R execute(P param);
}
