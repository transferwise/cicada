/** Run as postgres user on postgres database **/

-- Create cicada user with password 'randomstring'
CREATE ROLE cicada LOGIN ENCRYPTED PASSWORD 'md5a4bdbc5247bf7e877e24f2d66d99e07b' SUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;

-- Database: db_cicada

-- DROP DATABASE db_cicada;

CREATE DATABASE db_cicada
  WITH OWNER = cicada
       ENCODING = 'UTF8'
       TABLESPACE = pg_default
       LC_COLLATE = 'en_US.UTF-8'
       LC_CTYPE = 'en_US.UTF-8'
       CONNECTION LIMIT = -1;
