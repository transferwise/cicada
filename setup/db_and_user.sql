/** Run as postgres user on postgres database **/

/* Create cicada user with weak, local-dev password
!!! Use a strong password for your production system

CREATE ROLE cicada LOGIN PASSWORD 'randomstring' SUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
*/

-- Database: db_cicada

-- DROP DATABASE db_cicada;

CREATE DATABASE db_cicada
  WITH OWNER = cicada
       ENCODING = 'UTF8'
       TABLESPACE = pg_default
       CONNECTION LIMIT = -1;
