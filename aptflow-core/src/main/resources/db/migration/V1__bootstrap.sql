-- table required by db-scheduler
create table scheduled_tasks
(
    task_name            TEXT                     NOT NULL,
    task_instance        TEXT                     NOT NULL,
    task_data            bytea,
    execution_time       TIMESTAMP WITH TIME ZONE NOT NULL,
    picked               BOOLEAN                  NOT NULL,
    picked_by            TEXT,
    last_success         TIMESTAMP WITH TIME ZONE,
    last_failure         TIMESTAMP WITH TIME ZONE,
    consecutive_failures INT,
    last_heartbeat       TIMESTAMP WITH TIME ZONE,
    version              BIGINT                   NOT NULL,
    PRIMARY KEY (task_name, task_instance)
);

CREATE INDEX execution_time_idx ON scheduled_tasks (execution_time);
CREATE INDEX last_heartbeat_idx ON scheduled_tasks (last_heartbeat);

-- tables for AptWorkflow

CREATE TABLE workflow
(
    id         VARCHAR PRIMARY KEY,
    class_name VARCHAR   NOT NULL,
    input      bytea,
    created    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE workflow_run
(
    id                 VARCHAR PRIMARY KEY,
    workflow_id        VARCHAR,
    scheduled_event_id VARCHAR,
    started_event_id   VARCHAR,
    completed_event_id VARCHAR,
    output             bytea,
    created            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    archived           TIMESTAMP          DEFAULT NULL
);

CREATE TABLE activity
(
    workflow_run_id    VARCHAR   NOT NULL,
    name               VARCHAR   NOT NULL,
    started_event_id   VARCHAR,
    completed_event_id VARCHAR,
    output             bytea,
    created            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE signal
(
    workflow_run_id   VARCHAR   NOT NULL,
    name              VARCHAR   NOT NULL,
    waiting_event_id  VARCHAR,
    received_event_id VARCHAR,
    value             bytea,
    created           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE condition
(
    workflow_run_id    VARCHAR   NOT NULL,
    identifier         VARCHAR   NOT NULL,
    waiting_event_id   VARCHAR,
    satisfied_event_id VARCHAR,
    created            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sleep
(
    workflow_run_id    VARCHAR   NOT NULL,
    identifier         VARCHAR   NOT NULL,
    started_event_id   VARCHAR,
    completed_event_id VARCHAR,
    duration_in_millis bigint,
    created            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE event
(
    id              VARCHAR PRIMARY KEY,
    workflow_run_id VARCHAR   NOT NULL,
    category        VARCHAR,
    status          VARCHAR,
    timestamp       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE
OR REPLACE VIEW v_event_function_id AS
(
SELECT name AS function_id, started_event_id AS event_id
FROM activity
UNION
    SELECT name AS function_id, completed_event_id AS event_id
    FROM activity
UNION
    SELECT identifier AS function_id, waiting_event_id AS event_id
    FROM "condition"
UNION
    SELECT identifier AS function_id, satisfied_event_id AS event_id
    FROM "condition"
UNION
    SELECT name, waiting_event_id AS event_id
    FROM signal
UNION
    SELECT name, received_event_id AS event_id
    FROM signal
UNION
    SELECT identifier AS function_id, started_event_id AS event_id
    FROM sleep
UNION
    SELECT identifier AS function_id, completed_event_id AS event_id
    FROM sleep
UNION
    SELECT id AS function_id, scheduled_event_id AS event_id
    FROM workflow_run
UNION
   SELECT id AS function_id, started_event_id AS event_id
   FROM workflow_run
UNION
   SELECT id AS function_id, completed_event_id AS event_id
   FROM workflow_run
);

CREATE
OR REPLACE VIEW v_workflow_run_event as
SELECT event.*,
       efi.function_id
FROM event
         INNER JOIN v_event_function_id efi
                    ON event.id = efi.event_id;


CREATE
OR REPLACE VIEW v_workflow_run_function AS (
SELECT
    a.workflow_run_id,
    a.name AS function_id,
    'ACTIVITY' AS category,
    started.timestamp AS started,
    completed.timestamp AS completed
FROM activity a
LEFT JOIN event started on a.started_event_id = started.id
LEFT JOIN event completed on a.completed_event_id = completed.id

UNION
SELECT
    c.workflow_run_id,
    c.identifier AS function_id,
    'CONDITION' AS category,
    started.timestamp AS started,
    completed.timestamp AS completed
FROM "condition" c
LEFT JOIN event started on c.waiting_event_id = started.id
LEFT JOIN event completed on c.satisfied_event_id = completed.id

UNION
SELECT
    s.workflow_run_id,
    s.name AS function_id,
    'SIGNAL' AS category,
    started.timestamp AS started,
    completed.timestamp AS completed
FROM signal s
         LEFT JOIN event started on s.waiting_event_id = started.id
         LEFT JOIN event completed on s.received_event_id = completed.id

UNION
SELECT
    s.workflow_run_id,
    s.identifier AS function_id,
    'SLEEP' AS category,
    started.timestamp AS started,
    completed.timestamp AS completed
FROM sleep s
         LEFT JOIN event started on s.started_event_id = started.id
         LEFT JOIN event completed on s.completed_event_id = completed.id

);
