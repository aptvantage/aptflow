package aptvantage.aptflow.engine;

import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.google.common.flogger.FluentLogger;
import aptvantage.aptflow.api.RunnableWorkflow;
import aptvantage.aptflow.AptWorkflow;
import aptvantage.aptflow.model.Workflow;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public class SchedulerConfig {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    //TODO -- move all of scheduling activities into this class

    private static Set<Object> workflowDependencies;
    public static final OneTimeTask<CompleteSleepTaskInput> COMPLETE_SLEEP_TASK = Tasks.oneTime("CompleteSleepTask", CompleteSleepTaskInput.class)
            .execute((inst, ctx) -> {
                CompleteSleepTaskInput data = inst.getData();
                AptWorkflow.repository.sleepCompleted(data.workflowId(), data.sleepIdentifier());
                resumeExecution(data.workflowId());
            });
    public static final OneTimeTask<SignalWorkflowTaskInput> SIGNAL_WORKFLOW_TASK = Tasks.oneTime("SignalWorkflowTask", SignalWorkflowTaskInput.class)
            .execute((inst, ctx) -> {
                SignalWorkflowTaskInput data = inst.getData();
                AptWorkflow.repository.signalReceived(data.workflowId(), data.signalName(), data.signalValue());
                resumeExecution(data.workflowId());
            });
    public static final OneTimeTask<StartWorkflowTaskInput> RUN_WORKFLOW_TASK = Tasks.oneTime("RunWorkflowTask", StartWorkflowTaskInput.class)
            .onFailure((executionComplete, executionOperations) -> {
                //TODO - this should truly be an edge case (eg, exception handling had an unhandled exception,
                // but we still shouldn't die in silence
                executionOperations.remove();
            })
            .execute((inst, ctx) -> {
                StartWorkflowTaskInput data = inst.getData();
                AptWorkflow.repository.workflowStarted(data.workflowId());
                resumeExecution(data.workflowId());
            });

    public static void initialize(Set<Object> workflowDependencies) {
        SchedulerConfig.workflowDependencies = workflowDependencies;
    }

    private static void resumeExecution(String workflowId) {
        Workflow workflow = AptWorkflow.repository.getWorkflow(workflowId);
        try {
            RunnableWorkflow instance = instantiate(workflow.className());
            WorkflowExecutor.workflowId.set(workflowId);
            Object output = instance.execute(workflow.input());
            WorkflowExecutor.workflowId.remove();
            AptWorkflow.repository.workflowCompleted(workflowId, output);
            logger.atInfo().log("Workflow [%s] is complete", workflowId);
        } catch (AwaitingSignalException e) {
            logger.atInfo().log("Pausing execution of workflow [%s] to wait for signal [%s]", workflowId, e.getSignal());
        } catch (WorkflowStillSleepingException e) {
            logger.atInfo().log("Workflow [%s] has been sleeping [%s] for [%s] out of [%s]", workflowId, e.getIdentifier(), e.getElapsedSleepTime(), e.getNapTime());
        } catch (WorkflowSleepingException e) {
            logger.atInfo().log("Pausing execution of workflow [%s] to sleep [%s] for [%s]", workflowId, e.getIdentifier(), e.getNapTime());
        } catch (ConditionNotSatisfiedException e) {
            logger.atInfo().log("Pausing execution of workflow [%s] because condition [%s] is not satisfied", workflowId, e.getIdentifier());
        } catch (Exception e) {
            //TODO -- handle unexpected/unhandled failure encountered while evaluating workflow
            // this should not re-throw an exception, it should handle them all
            throw new RuntimeException(e);
        }
    }

    private static RunnableWorkflow instantiate(String className) {
        try {
            Class workflowClass = Class.forName(className);
            Constructor constructor = findInjectableConstructor(workflowClass);

            Object[] args = Arrays.stream(constructor.getParameterTypes())
                    .map(type -> workflowDependencies.stream()
                            .filter(o -> type.isAssignableFrom(o.getClass()))
                            .findFirst()
                            .orElseThrow(() -> new NoSuchElementException("Could not find a constructor arg match for type [%s] on class [%s]".formatted(type, workflowClass))))
                    .toArray();
            return (RunnableWorkflow) constructor.newInstance(args);

        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Class must have only one public constructor or have a single public constructor annotated with @Inject
     *
     * @param clazz
     * @return
     */
    private static Constructor findInjectableConstructor(Class clazz) {
        List<Constructor> list = Arrays.stream(clazz.getConstructors())
                .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
                .toList();
        if (list.size() == 1) {
            return list.get(0);
        }
        throw new RuntimeException("still need to handle multiple constructors with @Inject");
    }
}
