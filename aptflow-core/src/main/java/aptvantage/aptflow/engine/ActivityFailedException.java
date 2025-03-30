package aptvantage.aptflow.engine;

import aptvantage.aptflow.model.ActivityFunction;

import java.io.Serializable;

public class ActivityFailedException extends RuntimeException {
    private final ActivityFunction<? extends Serializable, ? extends Serializable, ? extends Serializable> activity;

    public <I extends Serializable, O extends Serializable, A extends Serializable>
    ActivityFailedException(ActivityFunction<I, O, A> activity, Throwable cause) {
        super(cause);
        this.activity = activity;
    }

    public ActivityFunction<? extends Serializable, ? extends Serializable, ? extends Serializable> getActivity() {
        return activity;
    }
}
