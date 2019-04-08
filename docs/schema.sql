/** Run as cicada user on db_cicada database **/
-- Add PostgreSQL extention required for uuid_generate_v1()
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- FUNCTION: public.set_auto_update_time()

-- DROP FUNCTION public.set_auto_update_time();

CREATE FUNCTION public.set_auto_update_time()
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

-- Table: global_settings

-- DROP TABLE global_settings;

CREATE TABLE global_settings
(
  global_setting_id serial NOT NULL,
  auto_update_time timestamp without time zone NOT NULL DEFAULT (now())::timestamp without time zone, -- auto populated datetime when the record last updated
  name character varying(64) NOT NULL,
  value character varying(64) NOT NULL,
  description character varying(255) NOT NULL,
  CONSTRAINT global_settings_pkey PRIMARY KEY (global_setting_id),
  CONSTRAINT global_settings_name_key UNIQUE (name)
)
WITH (
  OIDS=FALSE
);
COMMENT ON COLUMN global_settings.auto_update_time IS 'auto populated datetime when the record last updated';

CREATE TRIGGER tr_global_settings
    BEFORE UPDATE 
    ON public.global_settings
    FOR EACH ROW
    EXECUTE PROCEDURE public.set_auto_update_time()
;

-- Table: servers

-- DROP TABLE servers;

CREATE TABLE servers
(
  server_id serial NOT NULL,
  auto_update_time timestamp without time zone NOT NULL DEFAULT (now())::timestamp without time zone, -- auto populated datetime when the record last updated
  hostname character varying(255) NOT NULL,
  fqdn character varying(255) NOT NULL,
  ip4_address character varying(255) NOT NULL,
  is_enabled smallint NOT NULL DEFAULT 0, -- 0=Disabled 1=Enabled,
  CONSTRAINT servers_pkey PRIMARY KEY (server_id),
  CONSTRAINT servers_name_key UNIQUE (hostname)
)
WITH (
  OIDS=FALSE
);
COMMENT ON COLUMN servers.auto_update_time IS 'auto populated datetime when the record last updated';
COMMENT ON COLUMN public.servers.is_enabled IS '0=Disabled 1=Enabled';

CREATE TRIGGER tr_servers
    BEFORE UPDATE 
    ON public.servers
    FOR EACH ROW
    EXECUTE PROCEDURE public.set_auto_update_time()
;

-- Table: schedule_groups

-- DROP TABLE schedule_groups;

CREATE TABLE schedule_groups
(
  schedule_group_id serial NOT NULL,
  auto_update_time timestamp without time zone NOT NULL DEFAULT (now())::timestamp without time zone, -- auto populated datetime when the record last updated
  name character varying(16) NOT NULL,
  description character varying(255),
  CONSTRAINT schedule_groups_pkey PRIMARY KEY (schedule_group_id),
  CONSTRAINT schedule_groups_name_key UNIQUE (name)
)
WITH (
  OIDS=FALSE
);
COMMENT ON COLUMN schedule_groups.auto_update_time IS 'auto populated datetime when the record last updated';

CREATE TRIGGER tr_schedule_groups
    BEFORE UPDATE 
    ON public.schedule_groups
    FOR EACH ROW
    EXECUTE PROCEDURE public.set_auto_update_time()
;

INSERT INTO schedule_groups
  (schedule_group_id, name, description)
VALUES
  (1, 'Other', ''),
  (2, 'General Test', ''),
  (10, 'Pilelinewise job', '')
;

-- Table: schedules

-- DROP TABLE schedules;

CREATE TABLE schedules
(
  schedule_id integer NOT NULL,
  auto_update_time timestamp without time zone NOT NULL DEFAULT (now())::timestamp without time zone, -- auto populated datetime when the record last updated
  server_id integer NOT NULL, -- The one server where the job will run
  schedule_order integer NOT NULL, -- Run order for this schedule. Lowest is first. Async jobs will be executed all at once
  description character varying(255), -- Description of Schedule
  is_async smallint NOT NULL DEFAULT 1, -- 0=Disabled 1=Enabled | is_async jobs execute in parallel
  is_enabled smallint NOT NULL DEFAULT 0, -- 0=Disabled 1=Enabled
  is_running smallint NOT NULL DEFAULT 0, -- 0=No 1=Yes
  interval_mask character varying(14) NOT NULL, -- When to execute the command | Modeled on unix crontab (minute hour dom month dow)
  first_run_date timestamp(3) without time zone NOT NULL DEFAULT '1000-01-01 00:00:00'::timestamp without time zone, -- The job will not execute before this datetime
  last_run_date timestamp(3) without time zone NOT NULL DEFAULT '9999-12-31 23:59:59'::timestamp without time zone, -- The job will not execute after this datetime
  command character varying(255) NOT NULL, -- Command to execute
  parameters character varying(255), -- Exact string of parameters for command
  adhoc_execute smallint NOT NULL DEFAULT 0, -- 0=Disabled 1=Enabled | The job will execute at next minute, regardless of other schedule time settings
  adhoc_parameters character varying(255), -- If specified, will overwrite parameters
  schedule_group_id integer, -- Optional field to help group schedules
  CONSTRAINT schedules_pkey PRIMARY KEY (schedule_id),
  CONSTRAINT schedules_schedule_group_id_fkey FOREIGN KEY (schedule_group_id)
      REFERENCES schedule_groups (schedule_group_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
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
COMMENT ON COLUMN schedules.description IS 'Description of Schedule';
COMMENT ON COLUMN schedules.is_async IS '0=Disabled 1=Enabled | is_async jobs execute in parallel';
COMMENT ON COLUMN schedules.is_enabled IS '0=Disabled 1=Enabled';
COMMENT ON COLUMN schedules.is_running IS '0=No 1=Yes';
COMMENT ON COLUMN schedules.interval_mask IS 'When to execute the command | Modeled on unix crontab (minute hour dom month dow)';
COMMENT ON COLUMN schedules.first_run_date IS 'The job will not execute before this datetime';
COMMENT ON COLUMN schedules.last_run_date IS 'The job will not execute after this datetime';
COMMENT ON COLUMN schedules.command IS 'Command to execute';
COMMENT ON COLUMN schedules.parameters IS 'Exact string of parameters for command';
COMMENT ON COLUMN schedules.adhoc_execute IS '0=Disabled 1=Enabled | The job will execute at next minute, regardless of other schedule time settings';
COMMENT ON COLUMN schedules.adhoc_parameters IS 'If specified, will overwrite parameters for next run';
COMMENT ON COLUMN schedules.schedule_group_id IS 'Optional field to help group schedules';

-- Index: schedules_adhoc_execute_idx

-- DROP INDEX schedules_adhoc_execute_idx;

CREATE INDEX schedules_adhoc_execute_idx
  ON schedules
  USING btree
  (adhoc_execute);

-- Index: schedules_is_enabled_idx

-- DROP INDEX schedules_is_enabled_idx;

CREATE INDEX schedules_is_enabled_idx
  ON schedules
  USING btree
  (is_enabled);

-- Index: schedules_schedule_group_id_idx

-- DROP INDEX schedules_schedule_group_id_idx;

CREATE INDEX schedules_schedule_group_id_idx
  ON schedules
  USING btree
  (schedule_group_id);

-- Index: public.schedules_server_id_idx

-- DROP INDEX public.schedules_server_id_idx;

CREATE INDEX schedules_server_id_idx
  ON public.schedules
  USING btree
  (server_id);

CREATE TRIGGER tr_schedules
    BEFORE UPDATE 
    ON public.schedules
    FOR EACH ROW
    EXECUTE PROCEDURE public.set_auto_update_time()
;

-- Table: public.schedule_details

-- DROP TABLE public.schedule_details;

CREATE TABLE public.schedule_details
(
    schedule_details_id serial NOT NULL,
    schedule_id integer NOT NULL,
    name character varying(255) COLLATE pg_catalog."default",
    value character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT schedule_details_pkey PRIMARY KEY (schedule_details_id),
    CONSTRAINT schedule_details_schedule_id_name_key UNIQUE (schedule_id, name)
,
    CONSTRAINT schedule_details_schedule_id_fkey FOREIGN KEY (schedule_id)
        REFERENCES public.schedules (schedule_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)
WITH (
    OIDS = FALSE
)
;

-- Index: fki_schedule_id

-- DROP INDEX public.fki_schedule_id;

CREATE INDEX fki_schedule_id
    ON public.schedule_details USING btree
    (schedule_id)
;

-- Table: schedule_log_status

-- DROP TABLE schedule_log_status;

CREATE TABLE schedule_log_status
(
  schedule_log_status_id integer NOT NULL,
  constant character varying(30) NOT NULL, -- CONSTANT used by code
  name character varying(64) NOT NULL, -- Human Friendly Label
  CONSTRAINT schedule_log_status_pkey PRIMARY KEY (schedule_log_status_id),
  CONSTRAINT schedule_log_status_constant_key UNIQUE (constant)
)
WITH (
  OIDS=FALSE
);
COMMENT ON COLUMN schedule_log_status.constant IS 'CONSTANT used by code';
COMMENT ON COLUMN schedule_log_status.name IS 'Human Friendly Label';

INSERT INTO schedule_log_status
  (schedule_log_status_id, constant, name)
VALUES
  (0, 'SCHEDULE_UNKNOWN', 'Unknown'),
  (1, 'SCHEDULE_RUNNING', 'Running'),
  (2, 'SCHEDULE_COMPLETE', 'Complete'),
  (3, 'SCHEDULE_ERROR', 'Error')
;

-- Table: public.schedule_log

-- DROP TABLE public.schedule_log;

CREATE TABLE public.schedule_log
(
  schedule_log_id character varying(64) NOT NULL DEFAULT uuid_generate_v1(),
  auto_update_time timestamp without time zone NOT NULL DEFAULT (now())::timestamp without time zone, -- auto populated datetime when the record last updated
  server_id integer NOT NULL,
  schedule_id integer NOT NULL,
  full_command character varying(255) NOT NULL, -- full_command as executed by scheduler
  start_time timestamp(3) without time zone NOT NULL, -- ALWAYS use now() | datetime of when job started
  end_time timestamp(3) without time zone, -- datetime of when job ended
  returncode integer, -- returncode as provided by the executed script
  error_detail character varying(255) DEFAULT NULL::character varying,
  schedule_log_status_id integer NOT NULL DEFAULT 0,
  CONSTRAINT schedule_log_pkey PRIMARY KEY (schedule_log_id),
  CONSTRAINT schedule_log_schedule_id_fkey FOREIGN KEY (schedule_id)
      REFERENCES public.schedules (schedule_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT schedule_log_schedule_log_status_id_fkey FOREIGN KEY (schedule_log_status_id)
      REFERENCES public.schedule_log_status (schedule_log_status_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT schedule_log_server_id_fkey FOREIGN KEY (server_id)
      REFERENCES public.servers (server_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
COMMENT ON COLUMN public.schedules.auto_update_time IS 'auto populated datetime when the record last updated';
COMMENT ON COLUMN public.schedule_log.full_command IS 'full_command as executed by scheduler';
COMMENT ON COLUMN public.schedule_log.start_time IS 'ALWAYS use now() | datetime of when job started';
COMMENT ON COLUMN public.schedule_log.end_time IS 'datetime of when job ended';
COMMENT ON COLUMN public.schedule_log.returncode IS 'returncode as provided by the executed script';

-- Index: schedule_log_schedule_id_idx

-- DROP INDEX schedule_log_schedule_id_idx;

CREATE INDEX schedule_log_schedule_id_idx
  ON schedule_log
  USING btree
  (schedule_id);

-- Index: schedule_log_schedule_log_status_id_idx

-- DROP INDEX schedule_log_schedule_log_status_id_idx;

CREATE INDEX schedule_log_schedule_log_status_id_idx
  ON schedule_log
  USING btree
  (schedule_log_status_id);

-- Index: schedule_log_server_id_schedule_id_schedule_log_status_id_idx

-- DROP INDEX schedule_log_server_id_schedule_id_schedule_log_status_id_idx;

CREATE INDEX schedule_log_server_id_schedule_id_schedule_log_status_id_idx
  ON schedule_log
  USING btree
  (server_id, schedule_id, schedule_log_status_id);

-- Index: public.schedule_log_schedule_id_start_time_idx

-- DROP INDEX public.schedule_log_schedule_id_start_time_idx;

CREATE INDEX schedule_log_schedule_id_start_time_idx
  ON public.schedule_log
  USING btree
  (schedule_id, start_time);

CREATE TRIGGER tr_schedule_log
    BEFORE UPDATE 
    ON public.schedule_log
    FOR EACH ROW
    EXECUTE PROCEDURE public.set_auto_update_time()
;
