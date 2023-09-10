--
-- PostgreSQL database dump
--

-- Dumped from database version 11.1 (Debian 11.1-1.pgdg90+1)
-- Dumped by pg_dump version 11.1 (Debian 11.1-1.pgdg90+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;
SET default_tablespace = '';
SET default_with_oids = false;

--Setup database
DROP DATABASE IF EXISTS amelchukova;
CREATE DATABASE amelchukova;
\c amelchukova;

\echo 'LOADING database'


CREATE TABLE IF NOT EXISTS a_table (
  total_rides_count INTEGER,
  average_ride_distance DOUBLE PRECISION,
  ride_distance_std DOUBLE PRECISION,
  min_ride_distance INTEGER,
  max_ride_distance INTEGER
);