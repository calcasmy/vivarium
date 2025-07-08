-- This script sets up the PostgreSQL schema for Supabase.
-- It is designed to be idempotent, allowing it to be run multiple times
-- without causing errors if objects already exist.
--
-- Key changes for Supabase compatibility:
-- 1. Removed all 'ALTER ... OWNER TO ...' statements. Supabase handles ownership automatically.
-- 2. Added 'ALTER TABLE ... ENABLE ROW LEVEL SECURITY;' for all public tables.
-- 3. Added basic RLS policies to allow 'authenticated' users to SELECT data.
--    You will need to customize these policies based on your application's security requirements.
--    For example, to restrict access to only data owned by the user.

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

-- Drop existing foreign key constraints before dropping tables
-- This ensures that tables can be dropped without dependency issues
ALTER TABLE IF EXISTS ONLY public.climate_hour_data DROP CONSTRAINT IF EXISTS climate_hour_data_location_id_forecast_date_fkey;
ALTER TABLE IF EXISTS ONLY public.climate_hour_data DROP CONSTRAINT IF EXISTS climate_hour_data_condition_code_fkey;
ALTER TABLE IF EXISTS ONLY public.climate_forecast_day DROP CONSTRAINT IF EXISTS climate_forecast_day_location_id_fkey;
ALTER TABLE IF EXISTS ONLY public.climate_day_data DROP CONSTRAINT IF EXISTS climate_day_data_location_id_forecast_date_fkey;
ALTER TABLE IF EXISTS ONLY public.climate_day_data DROP CONSTRAINT IF EXISTS climate_day_data_condition_code_fkey;
ALTER TABLE IF EXISTS ONLY public.climate_astro_data DROP CONSTRAINT IF EXISTS climate_astro_data_location_id_forecast_date_fkey;

-- Drop indexes
DROP INDEX IF EXISTS public.idx_sensor_readings_timestamp;
DROP INDEX IF EXISTS public.idx_sensor_readings_sensor_id;
DROP INDEX IF EXISTS public.idx_sensor_id;
DROP INDEX IF EXISTS public.idx_raw_data_temperature;
DROP INDEX IF EXISTS public.idx_device_status_timestamp;
DROP INDEX IF EXISTS public.idx_device_status_device_id;
DROP INDEX IF EXISTS public.idx_device_id;

-- Drop existing unique and primary key constraints
ALTER TABLE IF EXISTS ONLY public.climate_location DROP CONSTRAINT IF EXISTS unique_lat_lon;
ALTER TABLE IF EXISTS ONLY public.climate_hour_data DROP CONSTRAINT IF EXISTS unique_hour_data;
ALTER TABLE IF EXISTS ONLY public.sensors DROP CONSTRAINT IF EXISTS sensors_sensor_name_key;
ALTER TABLE IF EXISTS ONLY public.sensors DROP CONSTRAINT IF EXISTS sensors_pkey;
ALTER TABLE IF EXISTS ONLY public.sensor_readings DROP CONSTRAINT IF EXISTS sensor_readings_pkey;
ALTER TABLE IF EXISTS ONLY public.raw_climate_data DROP CONSTRAINT IF EXISTS raw_climate_data_pkey;
ALTER TABLE IF EXISTS ONLY public.devices DROP CONSTRAINT IF EXISTS devices_pkey;
ALTER TABLE IF EXISTS ONLY public.devices DROP CONSTRAINT IF EXISTS devices_device_name_key;
ALTER TABLE IF EXISTS ONLY public.device_status DROP CONSTRAINT IF EXISTS device_status_pkey;
ALTER TABLE IF EXISTS ONLY public.climate_location DROP CONSTRAINT IF EXISTS climate_location_pkey;
ALTER TABLE IF EXISTS ONLY public.climate_forecast_day DROP CONSTRAINT IF EXISTS climate_forecast_day_pkey;
ALTER TABLE IF EXISTS ONLY public.climate_day_data DROP CONSTRAINT IF EXISTS climate_day_data_pkey;
ALTER TABLE IF EXISTS ONLY public.climate_condition DROP CONSTRAINT IF EXISTS climate_condition_pkey;
ALTER TABLE IF EXISTS ONLY public.climate_astro_data DROP CONSTRAINT IF EXISTS climate_astro_data_pkey;

-- Drop default values for sequence-owned columns
ALTER TABLE IF EXISTS public.sensors ALTER COLUMN sensor_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.sensor_readings ALTER COLUMN reading_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.devices ALTER COLUMN device_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.device_status ALTER COLUMN status_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.climate_location ALTER COLUMN location_id DROP DEFAULT;

-- Drop tables and sequences
DROP TABLE IF EXISTS public.sensors CASCADE;
DROP SEQUENCE IF EXISTS public.sensors_sensor_id_seq CASCADE;
DROP TABLE IF EXISTS public.sensor_readings CASCADE;
DROP SEQUENCE IF EXISTS public.sensor_readings_reading_id_seq CASCADE;
DROP TABLE IF EXISTS public.raw_climate_data CASCADE;
DROP TABLE IF EXISTS public.devices CASCADE;
DROP SEQUENCE IF EXISTS public.devices_device_id_seq CASCADE;
DROP TABLE IF EXISTS public.device_status CASCADE;
DROP SEQUENCE IF EXISTS public.device_status_status_id_seq CASCADE;
DROP TABLE IF EXISTS public.climate_location CASCADE;
DROP SEQUENCE IF EXISTS public.climate_location_location_id_seq CASCADE;
DROP TABLE IF EXISTS public.climate_hour_data CASCADE;
DROP TABLE IF EXISTS public.climate_forecast_day CASCADE;
DROP TABLE IF EXISTS public.climate_day_data CASCADE;
DROP TABLE IF EXISTS public.climate_condition CASCADE;
DROP TABLE IF EXISTS public.climate_astro_data CASCADE;

SET default_tablespace = '';
SET default_table_access_method = heap;

-- Create Tables
CREATE TABLE public.climate_astro_data (
    location_id integer NOT NULL,
    forecast_date date NOT NULL,
    sunrise character varying(10),
    sunset character varying(10),
    moonrise character varying(20),
    moonset character varying(20),
    moon_phase character varying(255),
    moon_illumination integer
);

CREATE TABLE public.climate_condition (
    condition_code integer NOT NULL,
    text character varying(255),
    icon character varying(255)
);

CREATE TABLE public.climate_day_data (
    location_id integer NOT NULL,
    forecast_date date NOT NULL,
    maxtemp_c numeric(5,2),
    maxtemp_f numeric(5,2),
    mintemp_c numeric(5,2),
    mintemp_f numeric(5,2),
    avgtemp_c numeric(5,2),
    avgtemp_f numeric(5,2),
    maxwind_mph numeric(5,2),
    maxwind_kph numeric(5,2),
    totalprecip_mm numeric(5,2),
    totalprecip_in numeric(5,2),
    totalsnow_cm numeric(5,2),
    avgvis_km numeric(5,2),
    avgvis_miles numeric(5,2),
    avghumidity integer,
    daily_will_it_rain integer,
    daily_chance_of_rain integer,
    daily_will_it_snow integer,
    daily_chance_of_snow integer,
    condition_code integer,
    uv numeric(3,1)
);

CREATE TABLE public.climate_forecast_day (
    location_id integer NOT NULL,
    forecast_date date NOT NULL,
    forecast_date_epoch bigint
);

CREATE TABLE public.climate_hour_data (
    location_id integer NOT NULL,
    forecast_date date NOT NULL,
    time_epoch bigint NOT NULL,
    "time" character varying(20),
    temp_c numeric(5,2),
    temp_f numeric(5,2),
    is_day integer,
    condition_code integer,
    wind_mph numeric(5,2),
    wind_kph numeric(5,2),
    wind_degree integer,
    wind_dir character varying(3),
    pressure_mb numeric(7,2),
    pressure_in numeric(7,2),
    precip_mm numeric(5,2),
    precip_in numeric(5,2),
    snow_cm numeric(5,2),
    humidity integer,
    cloud integer,
    feelslike_c numeric(5,2),
    feelslike_f numeric(5,2),
    windchill_c numeric(5,2),
    windchill_f numeric(5,2),
    heatindex_c numeric(5,2),
    heatindex_f numeric(5,2),
    dewpoint_c numeric(5,2),
    dewpoint_f numeric(5,2),
    will_it_rain integer,
    chance_of_rain integer,
    will_it_snow integer,
    chance_of_snow integer,
    vis_km numeric(5,2),
    vis_miles numeric(5,2),
    gust_mph numeric(5,2),
    gust_kph numeric(5,2),
    uv numeric(3,1)
);

CREATE TABLE public.climate_location (
    location_id integer NOT NULL,
    name character varying(255),
    region character varying(255),
    country character varying(255),
    latitude numeric(10,6),
    longitude numeric(10,6),
    timezone_id character varying(255),
    localtime_epoch bigint,
    "localtime" character varying(20)
);

CREATE TABLE public.device_status (
    status_id bigint NOT NULL,
    device_id integer NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    is_on boolean NOT NULL,
    device_data jsonb
);

CREATE TABLE public.devices (
    device_id integer NOT NULL,
    device_name character varying(255) NOT NULL,
    device_type character varying(50) NOT NULL,
    location character varying(100),
    model character varying(100),
    date_added date
);

CREATE TABLE public.raw_climate_data (
    weather_date date NOT NULL,
    raw_data jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE public.sensor_readings (
    reading_id bigint NOT NULL,
    sensor_id integer NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    raw_data jsonb
);

CREATE TABLE public.sensors (
    sensor_id integer NOT NULL,
    sensor_name character varying(255) NOT NULL,
    sensor_type character varying(50) NOT NULL,
    location character varying(100),
    model character varying(100),
    date_installed date
);

-- Create Sequences
CREATE SEQUENCE public.climate_location_location_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE public.device_status_status_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE public.devices_device_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE public.sensor_readings_reading_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE public.sensors_sensor_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Set sequence ownership and default values for columns
ALTER SEQUENCE public.climate_location_location_id_seq OWNED BY public.climate_location.location_id;
ALTER TABLE ONLY public.climate_location ALTER COLUMN location_id SET DEFAULT nextval('public.climate_location_location_id_seq'::regclass);

ALTER SEQUENCE public.device_status_status_id_seq OWNED BY public.device_status.status_id;
ALTER TABLE ONLY public.device_status ALTER COLUMN status_id SET DEFAULT nextval('public.device_status_status_id_seq'::regclass);

ALTER SEQUENCE public.devices_device_id_seq OWNED BY public.devices.device_id;
ALTER TABLE ONLY public.devices ALTER COLUMN device_id SET DEFAULT nextval('public.devices_device_id_seq'::regclass);

ALTER SEQUENCE public.sensor_readings_reading_id_seq OWNED BY public.sensor_readings.reading_id;
ALTER TABLE ONLY public.sensor_readings ALTER COLUMN reading_id SET DEFAULT nextval('public.sensor_readings_reading_id_seq'::regclass);

ALTER SEQUENCE public.sensors_sensor_id_seq OWNED BY public.sensors.sensor_id;
ALTER TABLE ONLY public.sensors ALTER COLUMN sensor_id SET DEFAULT nextval('public.sensors_sensor_id_seq'::regclass);

-- Add Primary Key Constraints
ALTER TABLE ONLY public.climate_astro_data ADD CONSTRAINT climate_astro_data_pkey PRIMARY KEY (location_id, forecast_date);
ALTER TABLE ONLY public.climate_condition ADD CONSTRAINT climate_condition_pkey PRIMARY KEY (condition_code);
ALTER TABLE ONLY public.climate_day_data ADD CONSTRAINT climate_day_data_pkey PRIMARY KEY (location_id, forecast_date);
ALTER TABLE ONLY public.climate_forecast_day ADD CONSTRAINT climate_forecast_day_pkey PRIMARY KEY (location_id, forecast_date);
ALTER TABLE ONLY public.climate_location ADD CONSTRAINT climate_location_pkey PRIMARY KEY (location_id);
ALTER TABLE ONLY public.device_status ADD CONSTRAINT device_status_pkey PRIMARY KEY (status_id);
ALTER TABLE ONLY public.devices ADD CONSTRAINT devices_pkey PRIMARY KEY (device_id);
ALTER TABLE ONLY public.raw_climate_data ADD CONSTRAINT raw_climate_data_pkey PRIMARY KEY (weather_date);
ALTER TABLE ONLY public.sensor_readings ADD CONSTRAINT sensor_readings_pkey PRIMARY KEY (reading_id);
ALTER TABLE ONLY public.sensors ADD CONSTRAINT sensors_pkey PRIMARY KEY (sensor_id);

-- Add Unique Constraints
ALTER TABLE ONLY public.devices ADD CONSTRAINT devices_device_name_key UNIQUE (device_name);
ALTER TABLE ONLY public.sensors ADD CONSTRAINT sensors_sensor_name_key UNIQUE (sensor_name);
ALTER TABLE ONLY public.climate_hour_data ADD CONSTRAINT unique_hour_data PRIMARY KEY (location_id, forecast_date, time_epoch);
ALTER TABLE ONLY public.climate_location ADD CONSTRAINT unique_lat_lon UNIQUE (latitude, longitude);

-- Create Indexes
CREATE INDEX idx_device_id ON public.devices USING btree (device_id);
CREATE INDEX idx_device_status_device_id ON public.device_status USING btree (device_id);
CREATE INDEX idx_device_status_timestamp ON public.device_status USING btree ("timestamp");
CREATE INDEX idx_raw_data_temperature ON public.sensor_readings USING gin (((raw_data -> 'temperature'::text)));
CREATE INDEX idx_sensor_id ON public.sensors USING btree (sensor_id);
CREATE INDEX idx_sensor_readings_sensor_id ON public.sensor_readings USING btree (sensor_id);
CREATE INDEX idx_sensor_readings_timestamp ON public.sensor_readings USING btree ("timestamp");

-- Add Foreign Key Constraints
ALTER TABLE ONLY public.climate_astro_data ADD CONSTRAINT climate_astro_data_location_id_forecast_date_fkey FOREIGN KEY (location_id, forecast_date) REFERENCES public.climate_forecast_day(location_id, forecast_date);
ALTER TABLE ONLY public.climate_day_data ADD CONSTRAINT climate_day_data_condition_code_fkey FOREIGN KEY (condition_code) REFERENCES public.climate_condition(condition_code);
ALTER TABLE ONLY public.climate_day_data ADD CONSTRAINT climate_day_data_location_id_forecast_date_fkey FOREIGN KEY (location_id, forecast_date) REFERENCES public.climate_forecast_day(location_id, forecast_date);
ALTER TABLE ONLY public.climate_forecast_day ADD CONSTRAINT climate_forecast_day_location_id_fkey FOREIGN KEY (location_id) REFERENCES public.climate_location(location_id);
ALTER TABLE ONLY public.climate_hour_data ADD CONSTRAINT climate_hour_data_condition_code_fkey FOREIGN KEY (condition_code) REFERENCES public.climate_condition(condition_code);
ALTER TABLE ONLY public.climate_hour_data ADD CONSTRAINT climate_hour_data_location_id_forecast_date_fkey FOREIGN KEY (location_id, forecast_date) REFERENCES public.climate_forecast_day(location_id, forecast_date);

-- Enable Row Level Security (RLS) on public tables
-- This is crucial for Supabase security. By default, with RLS enabled and no policies,
-- no one can access the data, which is secure but not functional.
ALTER TABLE public.climate_astro_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.climate_condition ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.climate_day_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.climate_forecast_day ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.climate_hour_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.climate_location ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.device_status ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.devices ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.raw_climate_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.sensor_readings ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.sensors ENABLE ROW LEVEL SECURITY;

-- Create RLS Policies (Example: Allow authenticated users to read data)
-- IMPORTANT: You MUST customize these policies based on your application's specific
-- authorization logic. These are very basic examples.

-- Policies for public.climate_astro_data
CREATE POLICY "Allow authenticated users to read climate_astro_data"
ON public.climate_astro_data
FOR SELECT
TO authenticated
USING (true);

-- Policies for public.climate_condition
CREATE POLICY "Allow authenticated users to read climate_condition"
ON public.climate_condition
FOR SELECT
TO authenticated
USING (true);

-- Policies for public.climate_day_data
CREATE POLICY "Allow authenticated users to read climate_day_data"
ON public.climate_day_data
FOR SELECT
TO authenticated
USING (true);

-- Policies for public.climate_forecast_day
CREATE POLICY "Allow authenticated users to read climate_forecast_day"
ON public.climate_forecast_day
FOR SELECT
TO authenticated
USING (true);

-- Policies for public.climate_hour_data
CREATE POLICY "Allow authenticated users to read climate_hour_data"
ON public.climate_hour_data
FOR SELECT
TO authenticated
USING (true);

-- Policies for public.climate_location
CREATE POLICY "Allow authenticated users to read climate_location"
ON public.climate_location
FOR SELECT
TO authenticated
USING (true);

-- Policies for public.device_status
CREATE POLICY "Allow authenticated users to read device_status"
ON public.device_status
FOR SELECT
TO authenticated
USING (true);

-- Policies for public.devices
CREATE POLICY "Allow authenticated users to read devices"
ON public.devices
FOR SELECT
TO authenticated
USING (true);

-- Policies for public.raw_climate_data
CREATE POLICY "Allow authenticated users to read raw_climate_data"
ON public.raw_climate_data
FOR SELECT
TO authenticated
USING (true);

-- Policies for public.sensor_readings
CREATE POLICY "Allow authenticated users to read sensor_readings"
ON public.sensor_readings
FOR SELECT
TO authenticated
USING (true);

-- Policies for public.sensors
CREATE POLICY "Allow authenticated users to read sensors"
ON public.sensors
FOR SELECT
TO authenticated
USING (true);

-- You might also want policies for INSERT, UPDATE, DELETE based on your needs.
-- Example for INSERT (allowing authenticated users to insert):
-- CREATE POLICY "Allow authenticated users to insert into devices"
-- ON public.devices
-- FOR INSERT
-- TO authenticated
-- WITH CHECK (true);

-- Example for UPDATE (allowing authenticated users to update their own devices, assuming a 'user_id' column)
-- CREATE POLICY "Allow users to update their own devices"
-- ON public.devices
-- FOR UPDATE
-- TO authenticated
-- USING (auth.uid() = user_id);
-- WITH CHECK (auth.uid() = user_id);
