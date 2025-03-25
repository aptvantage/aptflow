package aptvantage.aptflow.engine;

import aptvantage.aptflow.model.Activity;

public class ActivityFailedException extends RuntimeException {
    private final Activity activity;

    public ActivityFailedException(Activity activity, Throwable cause) {
        super(cause);
        this.activity = activity;
    }

    public Activity getActivity() {
        return activity;
    }
}
