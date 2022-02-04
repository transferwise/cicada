/** Run as cicada user on db_cicada database **/
START TRANSACTION;

CREATE SCHEMA IF NOT EXISTS public;

-- Add PostgreSQL extention required for uuid_generate_v1()
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- FUNCTION: set_auto_update_time()
CREATE OR REPLACE FUNCTION public.set_auto_update_time()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
  NEW.auto_update_time = now()::timestamp without time zone;
  RETURN NEW;
END;
$BODY$;

-- Table: servers
CREATE TABLE IF NOT EXISTS public.servers
(
  server_id serial NOT NULL,
  auto_update_time timestamp without time zone NOT NULL DEFAULT (now())::timestamp without time zone,
  hostname character varying(255) NOT NULL,
  fqdn character varying(255) NOT NULL,
  ip4_address character varying(255) NOT NULL,
  is_enabled smallint NOT NULL DEFAULT 0,
  CONSTRAINT servers_pkey PRIMARY KEY (server_id),
  CONSTRAINT servers_name_key UNIQUE (hostname),
  CONSTRAINT servers_ip4_address_key UNIQUE (ip4_address)
)
WITH (
  OIDS=FALSE
);
COMMENT ON COLUMN servers.auto_update_time IS 'auto populated datetime when the record last updated';
COMMENT ON COLUMN servers.is_enabled IS '0=Disabled 1=Enabled';

DROP TRIGGER IF EXISTS tr_servers ON public.servers;
CREATE TRIGGER tr_servers
    BEFORE UPDATE
    ON public.servers
    FOR EACH ROW
    EXECUTE PROCEDURE set_auto_update_time()
;

-- Table: schedules
CREATE TABLE IF NOT EXISTS public.schedules
(
  auto_update_time timestamp without time zone NOT NULL DEFAULT (now())::timestamp without time zone,
  schedule_id character varying(255) NOT NULL DEFAULT uuid_generate_v1(),
  schedule_description character varying(255),
  server_id integer NOT NULL,
  schedule_order integer NOT NULL,
  is_async smallint NOT NULL DEFAULT 1,
  is_enabled smallint NOT NULL DEFAULT 0,
  adhoc_execute smallint NOT NULL DEFAULT 0,
  is_running smallint NOT NULL DEFAULT 0,
  abort_running smallint NOT NULL DEFAULT 0,
  interval_mask character varying(32) NOT NULL,
  first_run_date timestamp(3) without time zone NOT NULL DEFAULT '1000-01-01 00:00:00.000'::timestamp without time zone,
  last_run_date timestamp(3) without time zone NOT NULL DEFAULT '9999-12-31 23:59:59.999'::timestamp without time zone,
  exec_command character varying(1024) NOT NULL,
  parameters character varying(255),
  adhoc_parameters character varying(255),
  schedule_group_id integer,
  CONSTRAINT schedules_pkey PRIMARY KEY (schedule_id),
  CONSTRAINT schedules_server_id_fkey FOREIGN KEY (server_id)
      REFERENCES servers (server_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
COMMENT ON COLUMN schedules.auto_update_time IS 'auto populated datetime when the record last updated';
COMMENT ON COLUMN schedules.server_id IS 'The one server where the job will run';
COMMENT ON COLUMN schedules.schedule_order IS 'Run order for this schedule. Lowest is first. Async jobs will be executed all at once';
COMMENT ON COLUMN schedules.schedule_description IS 'Schedule Description and Comments';
COMMENT ON COLUMN schedules.is_async IS '0=Disabled 1=Enabled | is_async jobs execute in parallel';
COMMENT ON COLUMN schedules.is_enabled IS '0=Disabled 1=Enabled';
COMMENT ON COLUMN schedules.adhoc_execute IS '0=Disabled 1=Enabled | The job will execute at next minute, regardless of other schedule time settings';
COMMENT ON COLUMN schedules.is_running IS '0=No 1=Yes';
COMMENT ON COLUMN schedules.abort_running IS '0=Disabled 1=Enabled | If the job is running, it will be terminated as soon as possible';
COMMENT ON COLUMN schedules.interval_mask IS 'When to execute the command | Modeled on unix crontab (minute hour dom month dow)';
COMMENT ON COLUMN schedules.first_run_date IS 'The job will not execute before this datetime';
COMMENT ON COLUMN schedules.last_run_date IS 'The job will not execute after this datetime';
COMMENT ON COLUMN schedules.exec_command IS 'Command to execute';
COMMENT ON COLUMN schedules.parameters IS 'Exact string of parameters for command';
COMMENT ON COLUMN schedules.adhoc_parameters IS 'If specified, will overwrite parameters for next run';
COMMENT ON COLUMN schedules.schedule_group_id IS 'Optional field to help group schedules';

-- Index: schedules_adhoc_execute_idx
CREATE INDEX IF NOT EXISTS schedules_adhoc_execute_idx
  ON public.schedules
  USING btree
  (adhoc_execute);

-- Index: schedules_is_enabled_idx
CREATE INDEX IF NOT EXISTS schedules_is_enabled_idx
  ON public.schedules
  USING btree
  (is_enabled);

-- Index: schedules_schedule_group_id_idx
CREATE INDEX IF NOT EXISTS schedules_schedule_group_id_idx
  ON public.schedules
  USING btree
  (schedule_group_id);

-- Index: schedules_server_id_idx
CREATE INDEX IF NOT EXISTS schedules_server_id_idx
  ON public.schedules
  USING btree
  (server_id);

DROP TRIGGER IF EXISTS tr_schedules ON public.schedules;
CREATE TRIGGER tr_schedules
    BEFORE UPDATE
    ON public.schedules
    FOR EACH ROW
    EXECUTE PROCEDURE set_auto_update_time()
;

-- Table: schedule_log
CREATE TABLE IF NOT EXISTS public.schedule_log
(
  schedule_log_id character varying(64) NOT NULL DEFAULT uuid_generate_v1(),
  auto_update_time timestamp without time zone NOT NULL DEFAULT (now())::timestamp without time zone,
  server_id integer NOT NULL,
  schedule_id character varying(255) NOT NULL,
  full_command character varying(1024) NOT NULL,
  start_time timestamp(3) without time zone NOT NULL,
  end_time timestamp(3) without time zone,
  returncode integer,
  error_detail character varying(255) DEFAULT NULL::character varying,
  CONSTRAINT schedule_log_pkey PRIMARY KEY (schedule_log_id),
  CONSTRAINT schedule_log_server_id_fkey FOREIGN KEY (server_id)
      REFERENCES servers (server_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
COMMENT ON COLUMN schedules.auto_update_time IS 'auto populated datetime when the record last updated';
COMMENT ON COLUMN schedule_log.full_command IS 'full_command as executed by scheduler';
COMMENT ON COLUMN schedule_log.start_time IS 'ALWAYS use now() | datetime of when job started';
COMMENT ON COLUMN schedule_log.end_time IS 'datetime of when job ended';
COMMENT ON COLUMN schedule_log.returncode IS 'returncode as provided by the executed script';

-- Index: schedule_log_schedule_id_idx
CREATE INDEX IF NOT EXISTS schedule_log_schedule_id_idx
  ON public.schedule_log
  USING btree
  (schedule_id);

-- Index: schedule_log_schedule_id_start_time_idx
CREATE INDEX IF NOT EXISTS schedule_log_schedule_id_start_time_idx
  ON public.schedule_log
  USING btree
  (schedule_id, start_time);

DROP TRIGGER IF EXISTS tr_schedule_log ON public.schedule_log;
CREATE TRIGGER tr_schedule_log
    BEFORE UPDATE
    ON public.schedule_log
    FOR EACH ROW
    EXECUTE PROCEDURE set_auto_update_time()
;

-- Table: schedule_log_historical
CREATE TABLE IF NOT EXISTS public.schedule_log_historical
(
    schedule_log_id character varying(64) NOT NULL,
    auto_update_time timestamp without time zone NOT NULL,
    server_id integer NOT NULL,
    schedule_id character varying(255) NOT NULL,
    full_command character varying(255) NOT NULL,
    start_time timestamp(3) without time zone NOT NULL,
    end_time timestamp(3) without time zone,
    returncode integer,
    error_detail character varying(255),
    CONSTRAINT schedule_log_historical_pkey PRIMARY KEY (schedule_log_id)
)
WITH (
    OIDS = FALSE
)
;

COMMIT TRANSACTION;
