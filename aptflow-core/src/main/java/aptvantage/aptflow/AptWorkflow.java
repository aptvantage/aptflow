package aptvantage.aptflow;

import aptvantage.aptflow.api.RunnableWorkflow;
import aptvantage.aptflow.api.WorkflowFunctions;
import aptvantage.aptflow.engine.WorkflowExecutor;
import aptvantage.aptflow.engine.persistence.WorkflowRepository;
import aptvantage.aptflow.model.Event;
import aptvantage.aptflow.model.WorkflowStatus;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.jdbi.v3.core.Jdbi;

import javax.sql.DataSource;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AptWorkflow {

    public static WorkflowRepository repository;
    private final WorkflowExecutor workflowExecutor;
    private final AptWorkflowBuilder builder;

    private AptWorkflow(WorkflowExecutor workflowExecutor, AptWorkflowBuilder builder) {
        this.workflowExecutor = workflowExecutor;
        this.builder = builder;
    }

    public static AptWorkflowBuilder builder() {
        return new AptWorkflowBuilder();
    }

    public <T extends Serializable> void signalWorkflow(String workflowId, String signalName, T signalValue) {
        this.workflowExecutor.signalWorkflow(workflowId, signalName, signalValue);
    }

    public <P extends Serializable> void runWorkflow(Class<? extends RunnableWorkflow<?, P>> workflowClass, P workflowParam, String workflowId) {
        this.workflowExecutor.runWorkflow(workflowClass, workflowParam, workflowId);
    }

    public <R> R getWorkflowOutput(String workflowId, Class<R> outputClass) {
        return (R) repository.getWorkflow(workflowId).output();
    }

    public boolean isWorkflowCompleted(String workflowId) {
        return repository.getWorkflow(workflowId).isComplete();
    }

    public WorkflowStatus getWorkflowStatus(String workflowId) {
        return repository.getWorkflowStatus(workflowId);
    }

    public List<Event> getWorkflowEvents(String workflowId) {
        return repository.getWorkflowEvents(workflowId);
    }

    public void stop() {
        this.workflowExecutor.stop();
        this.builder.stop();
    }

    public static class AptWorkflowBuilder {

        private final Set<Object> workflowDependencies = new HashSet<>();
        private DataSource dataSource;

        private boolean managedDataSource = false;

        private AptWorkflowBuilder() {
        }

        private static void runDatabaseMigration(DataSource dataSource) {
            MigrateResult migrate = Flyway.configure()
                    .baselineVersion("0")
                    .baselineOnMigrate(true)
                    .dataSource(dataSource)
                    .load()
                    .migrate();
        }

        private static HikariDataSource initializeDataSource(String username, String password, String url) {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(url);
            config.setUsername(username);
            config.setPassword(password);
            config.setDriverClassName("org.postgresql.Driver");
            config.setMaximumPoolSize(10);

            // setting min idle prevents the datasource from eagerly creating max pool size
            config.setMinimumIdle(2);
            config.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
            return new HikariDataSource(config);
        }

        public AptWorkflowBuilder dataSource(String username, String password, String url) {
            this.dataSource = initializeDataSource(username, password, url);
            this.managedDataSource = true;
            return this;
        }

        public AptWorkflowBuilder dataSource(DataSource dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        public AptWorkflowBuilder registerWorkflowDependencies(Object... objects) {
            workflowDependencies.addAll(Arrays.asList(objects));
            return this;
        }

        public AptWorkflow start() {
            //TODO -- null check this.dataSource
            runDatabaseMigration(this.dataSource);

            AptWorkflow.repository = new WorkflowRepository(Jdbi.create(this.dataSource));
            WorkflowExecutor executor = new WorkflowExecutor(
                    this.dataSource,
                    AptWorkflow.repository,
                    workflowDependencies);

            WorkflowFunctions.initialize(executor);

            // start this (last) after the rest of the app is completely initialized
            executor.start();
            return new AptWorkflow(executor, this);
        }


        public void stop() {
            if (managedDataSource) {
                ((HikariDataSource) this.dataSource).close();
            }
        }
    }
}
