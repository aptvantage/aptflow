package aptvantage.aptflow;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.Task;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import aptvantage.aptflow.engine.WorkflowExecutor;
import aptvantage.aptflow.api.WorkflowFunctions;
import aptvantage.aptflow.engine.SchedulerConfig;
import aptvantage.aptflow.engine.persistence.WorkflowRepository;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.jdbi.v3.core.Jdbi;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class AptWorkflow {

    public static WorkflowRepository repository;
    private final WorkflowExecutor workflowExecutor;
    private final NimbusServiceBuilder builder;

    private AptWorkflow(WorkflowExecutor workflowExecutor, NimbusServiceBuilder builder) {
        this.workflowExecutor = workflowExecutor;
        this.builder = builder;
    }

    public static NimbusServiceBuilder builder() {
        return new NimbusServiceBuilder();
    }

    public WorkflowExecutor executor() {
        return this.workflowExecutor;
    }

    public void stop() {
        builder.stop();
    }

    public static class NimbusServiceBuilder {

        private final Set<Object> workflowDependencies = new HashSet<>();
        private DataSource dataSource;

        private boolean managedDataSource = false;
        private Scheduler scheduler;

        private NimbusServiceBuilder() {
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

        public NimbusServiceBuilder dataSource(String username, String password, String url) {
            this.dataSource = initializeDataSource(username, password, url);
            this.managedDataSource = true;
            return this;
        }

        public NimbusServiceBuilder dataSource(DataSource dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        public NimbusServiceBuilder registerWorkflowDependencies(Object... objects) {
            workflowDependencies.addAll(Arrays.asList(objects));
            return this;
        }

        public AptWorkflow start() {
            //TODO -- null check this.dataSource
            runDatabaseMigration(this.dataSource);

            SchedulerConfig.initialize(this.workflowDependencies);

            this.scheduler = initializeScheduler(this.dataSource,
                    SchedulerConfig.RUN_WORKFLOW_TASK,
                    SchedulerConfig.SIGNAL_WORKFLOW_TASK,
                    SchedulerConfig.COMPLETE_SLEEP_TASK);

            WorkflowExecutor executor = new WorkflowExecutor(scheduler);
            AptWorkflow.repository = new WorkflowRepository(Jdbi.create(this.dataSource));
            WorkflowFunctions.initialize(scheduler);

            // start this (last) after the rest of the app is completely initialized
            this.scheduler.start();
            return new AptWorkflow(executor, this);
        }

        private Scheduler initializeScheduler(DataSource dataSource, Task... tasks) {
            Scheduler scheduler = Scheduler
                    .create(dataSource, tasks)
                    .pollingInterval(Duration.ofSeconds(1))
                    .enableImmediateExecution()
                    .build();

            return scheduler;
        }

        public void stop() {
            this.scheduler.stop();
            if (managedDataSource) {
                ((HikariDataSource) this.dataSource).close();
            }
        }
    }
}
