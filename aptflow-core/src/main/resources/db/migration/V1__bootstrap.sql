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


-- tables for nimble workflow
CREATE TABLE workflow
(
    id                  VARCHAR PRIMARY KEY,
    scheduled_event_id  VARCHAR,
    started_event_id    VARCHAR,
    completed_event_id  VARCHAR,
    class_name          VARCHAR,
    input               bytea,
    output              bytea,
    created             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE activity
(
    workflow_id         VARCHAR NOT NULL,
    name                VARCHAR NOT NULL,
    started_event_id    VARCHAR,
    completed_event_id  VARCHAR,
    output              bytea,
    created             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE signal
(
    workflow_id         VARCHAR NOT NULL,
    name                VARCHAR NOT NULL,
    waiting_event_id    VARCHAR,
    received_event_id   VARCHAR,
    value               bytea,
    created             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE condition
(
    workflow_id         VARCHAR NOT NULL,
    identifier          VARCHAR NOT NULL,
    waiting_event_id    VARCHAR,
    satisfied_event_id  VARCHAR,
    created             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sleep
(
    workflow_id         VARCHAR NOT NULL,
    identifier          VARCHAR NOT NULL,
    started_event_id    VARCHAR,
    completed_event_id  VARCHAR,
    duration_in_millis  bigint,
    created             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE event
(
    id              VARCHAR PRIMARY KEY,
    workflow_id     VARCHAR NOT NULL,
    category        VARCHAR,
    status          VARCHAR,
    timestamp       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE VIEW v_event_function_identifier AS
(
SELECT name AS identifier, started_event_id AS event_id
FROM activity
UNION
    SELECT name AS identifier, completed_event_id AS event_id
    FROM activity
UNION
    SELECT identifier, waiting_event_id AS event_id
    FROM "condition"
UNION
    SELECT identifier, satisfied_event_id AS event_id
    FROM "condition"
UNION
    SELECT name, waiting_event_id AS event_id
    FROM signal
UNION
    SELECT name, received_event_id AS event_id
    FROM signal
UNION
    SELECT identifier, started_event_id AS event_id
    FROM sleep
UNION
    SELECT identifier, completed_event_id AS event_id
    FROM sleep
UNION
    SELECT id AS identifier, scheduled_event_id AS event_id
    FROM workflow
UNION
   SELECT id AS identifier, started_event_id AS event_id
   FROM workflow
UNION
   SELECT id AS identifier, completed_event_id AS event_id
   FROM workflow
);

CREATE OR REPLACE VIEW v_workflow_event as
SELECT event.*,
       vfi.identifier
FROM event
         INNER JOIN v_event_function_identifier vfi
                    ON event.id = vfi.event_id;
