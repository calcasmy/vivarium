--
-- PostgreSQL database dump
--

-- Dumped from database version 15.13 (Debian 15.13-0+deb12u1)
-- Dumped by pg_dump version 15.13 (Debian 15.13-0+deb12u1)

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

ALTER TABLE IF EXISTS ONLY public.climate_hour_data DROP CONSTRAINT IF EXISTS climate_hour_data_location_id_forecast_date_fkey;
ALTER TABLE IF EXISTS ONLY public.climate_hour_data DROP CONSTRAINT IF EXISTS climate_hour_data_condition_code_fkey;
ALTER TABLE IF EXISTS ONLY public.climate_forecast_day DROP CONSTRAINT IF EXISTS climate_forecast_day_location_id_fkey;
ALTER TABLE IF EXISTS ONLY public.climate_day_data DROP CONSTRAINT IF EXISTS climate_day_data_location_id_forecast_date_fkey;
ALTER TABLE IF EXISTS ONLY public.climate_day_data DROP CONSTRAINT IF EXISTS climate_day_data_condition_code_fkey;
ALTER TABLE IF EXISTS ONLY public.climate_astro_data DROP CONSTRAINT IF EXISTS climate_astro_data_location_id_forecast_date_fkey;
DROP INDEX IF EXISTS public.idx_sensor_readings_timestamp;
DROP INDEX IF EXISTS public.idx_sensor_readings_sensor_id;
DROP INDEX IF EXISTS public.idx_sensor_id;
DROP INDEX IF EXISTS public.idx_raw_data_temperature;
DROP INDEX IF EXISTS public.idx_device_status_timestamp;
DROP INDEX IF EXISTS public.idx_device_status_device_id;
DROP INDEX IF EXISTS public.idx_device_id;
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
ALTER TABLE IF EXISTS public.sensors ALTER COLUMN sensor_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.sensor_readings ALTER COLUMN reading_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.devices ALTER COLUMN device_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.device_status ALTER COLUMN status_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.climate_location ALTER COLUMN location_id DROP DEFAULT;
DROP SEQUENCE IF EXISTS public.sensors_sensor_id_seq;
DROP TABLE IF EXISTS public.sensors;
DROP SEQUENCE IF EXISTS public.sensor_readings_reading_id_seq;
DROP TABLE IF EXISTS public.sensor_readings;
DROP TABLE IF EXISTS public.raw_climate_data;
DROP SEQUENCE IF EXISTS public.devices_device_id_seq;
DROP TABLE IF EXISTS public.devices;
DROP SEQUENCE IF EXISTS public.device_status_status_id_seq;
DROP TABLE IF EXISTS public.device_status;
DROP SEQUENCE IF EXISTS public.climate_location_location_id_seq;
DROP TABLE IF EXISTS public.climate_location;
DROP TABLE IF EXISTS public.climate_hour_data;
DROP TABLE IF EXISTS public.climate_forecast_day;
DROP TABLE IF EXISTS public.climate_day_data;
DROP TABLE IF EXISTS public.climate_condition;
DROP TABLE IF EXISTS public.climate_astro_data;
SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: climate_astro_data; Type: TABLE; Schema: public; Owner: vivarium
--

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


ALTER TABLE public.climate_astro_data OWNER TO vivarium;

--
-- Name: climate_condition; Type: TABLE; Schema: public; Owner: vivarium
--

CREATE TABLE public.climate_condition (
    condition_code integer NOT NULL,
    text character varying(255),
    icon character varying(255)
);


ALTER TABLE public.climate_condition OWNER TO vivarium;

--
-- Name: climate_day_data; Type: TABLE; Schema: public; Owner: vivarium
--

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


ALTER TABLE public.climate_day_data OWNER TO vivarium;

--
-- Name: climate_forecast_day; Type: TABLE; Schema: public; Owner: vivarium
--

CREATE TABLE public.climate_forecast_day (
    location_id integer NOT NULL,
    forecast_date date NOT NULL,
    forecast_date_epoch bigint
);


ALTER TABLE public.climate_forecast_day OWNER TO vivarium;

--
-- Name: climate_hour_data; Type: TABLE; Schema: public; Owner: vivarium
--

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


ALTER TABLE public.climate_hour_data OWNER TO vivarium;

--
-- Name: climate_location; Type: TABLE; Schema: public; Owner: vivarium
--

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


ALTER TABLE public.climate_location OWNER TO vivarium;

--
-- Name: climate_location_location_id_seq; Type: SEQUENCE; Schema: public; Owner: vivarium
--

CREATE SEQUENCE public.climate_location_location_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.climate_location_location_id_seq OWNER TO vivarium;

--
-- Name: climate_location_location_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: vivarium
--

ALTER SEQUENCE public.climate_location_location_id_seq OWNED BY public.climate_location.location_id;


--
-- Name: device_status; Type: TABLE; Schema: public; Owner: vivarium
--

CREATE TABLE public.device_status (
    status_id bigint NOT NULL,
    device_id integer NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    is_on boolean NOT NULL,
    device_data jsonb
);


ALTER TABLE public.device_status OWNER TO vivarium;

--
-- Name: device_status_status_id_seq; Type: SEQUENCE; Schema: public; Owner: vivarium
--

CREATE SEQUENCE public.device_status_status_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.device_status_status_id_seq OWNER TO vivarium;

--
-- Name: device_status_status_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: vivarium
--

ALTER SEQUENCE public.device_status_status_id_seq OWNED BY public.device_status.status_id;


--
-- Name: devices; Type: TABLE; Schema: public; Owner: vivarium
--

CREATE TABLE public.devices (
    device_id integer NOT NULL,
    device_name character varying(255) NOT NULL,
    device_type character varying(50) NOT NULL,
    location character varying(100),
    model character varying(100),
    date_added date
);


ALTER TABLE public.devices OWNER TO vivarium;

--
-- Name: devices_device_id_seq; Type: SEQUENCE; Schema: public; Owner: vivarium
--

CREATE SEQUENCE public.devices_device_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.devices_device_id_seq OWNER TO vivarium;

--
-- Name: devices_device_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: vivarium
--

ALTER SEQUENCE public.devices_device_id_seq OWNED BY public.devices.device_id;


--
-- Name: raw_climate_data; Type: TABLE; Schema: public; Owner: vivarium
--

CREATE TABLE public.raw_climate_data (
    weather_date date NOT NULL,
    raw_data jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.raw_climate_data OWNER TO vivarium;

--
-- Name: sensor_readings; Type: TABLE; Schema: public; Owner: vivarium
--

CREATE TABLE public.sensor_readings (
    reading_id bigint NOT NULL,
    sensor_id integer NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    raw_data jsonb
);


ALTER TABLE public.sensor_readings OWNER TO vivarium;

--
-- Name: sensor_readings_reading_id_seq; Type: SEQUENCE; Schema: public; Owner: vivarium
--

CREATE SEQUENCE public.sensor_readings_reading_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.sensor_readings_reading_id_seq OWNER TO vivarium;

--
-- Name: sensor_readings_reading_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: vivarium
--

ALTER SEQUENCE public.sensor_readings_reading_id_seq OWNED BY public.sensor_readings.reading_id;


--
-- Name: sensors; Type: TABLE; Schema: public; Owner: vivarium
--

CREATE TABLE public.sensors (
    sensor_id integer NOT NULL,
    sensor_name character varying(255) NOT NULL,
    sensor_type character varying(50) NOT NULL,
    location character varying(100),
    model character varying(100),
    date_installed date
);


ALTER TABLE public.sensors OWNER TO vivarium;

--
-- Name: sensors_sensor_id_seq; Type: SEQUENCE; Schema: public; Owner: vivarium
--

CREATE SEQUENCE public.sensors_sensor_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.sensors_sensor_id_seq OWNER TO vivarium;

--
-- Name: sensors_sensor_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: vivarium
--

ALTER SEQUENCE public.sensors_sensor_id_seq OWNED BY public.sensors.sensor_id;


--
-- Name: climate_location location_id; Type: DEFAULT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.climate_location ALTER COLUMN location_id SET DEFAULT nextval('public.climate_location_location_id_seq'::regclass);


--
-- Name: device_status status_id; Type: DEFAULT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.device_status ALTER COLUMN status_id SET DEFAULT nextval('public.device_status_status_id_seq'::regclass);


--
-- Name: devices device_id; Type: DEFAULT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.devices ALTER COLUMN device_id SET DEFAULT nextval('public.devices_device_id_seq'::regclass);


--
-- Name: sensor_readings reading_id; Type: DEFAULT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.sensor_readings ALTER COLUMN reading_id SET DEFAULT nextval('public.sensor_readings_reading_id_seq'::regclass);


--
-- Name: sensors sensor_id; Type: DEFAULT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.sensors ALTER COLUMN sensor_id SET DEFAULT nextval('public.sensors_sensor_id_seq'::regclass);


--
-- Name: climate_astro_data climate_astro_data_pkey; Type: CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.climate_astro_data
    ADD CONSTRAINT climate_astro_data_pkey PRIMARY KEY (location_id, forecast_date);


--
-- Name: climate_condition climate_condition_pkey; Type: CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.climate_condition
    ADD CONSTRAINT climate_condition_pkey PRIMARY KEY (condition_code);


--
-- Name: climate_day_data climate_day_data_pkey; Type: CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.climate_day_data
    ADD CONSTRAINT climate_day_data_pkey PRIMARY KEY (location_id, forecast_date);


--
-- Name: climate_forecast_day climate_forecast_day_pkey; Type: CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.climate_forecast_day
    ADD CONSTRAINT climate_forecast_day_pkey PRIMARY KEY (location_id, forecast_date);


--
-- Name: climate_location climate_location_pkey; Type: CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.climate_location
    ADD CONSTRAINT climate_location_pkey PRIMARY KEY (location_id);


--
-- Name: device_status device_status_pkey; Type: CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.device_status
    ADD CONSTRAINT device_status_pkey PRIMARY KEY (status_id);


--
-- Name: devices devices_device_name_key; Type: CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.devices
    ADD CONSTRAINT devices_device_name_key UNIQUE (device_name);


--
-- Name: devices devices_pkey; Type: CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.devices
    ADD CONSTRAINT devices_pkey PRIMARY KEY (device_id);


--
-- Name: raw_climate_data raw_climate_data_pkey; Type: CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.raw_climate_data
    ADD CONSTRAINT raw_climate_data_pkey PRIMARY KEY (weather_date);


--
-- Name: sensor_readings sensor_readings_pkey; Type: CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.sensor_readings
    ADD CONSTRAINT sensor_readings_pkey PRIMARY KEY (reading_id);


--
-- Name: sensors sensors_pkey; Type: CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.sensors
    ADD CONSTRAINT sensors_pkey PRIMARY KEY (sensor_id);


--
-- Name: sensors sensors_sensor_name_key; Type: CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.sensors
    ADD CONSTRAINT sensors_sensor_name_key UNIQUE (sensor_name);


--
-- Name: climate_hour_data unique_hour_data; Type: CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.climate_hour_data
    ADD CONSTRAINT unique_hour_data PRIMARY KEY (location_id, forecast_date, time_epoch);


--
-- Name: climate_location unique_lat_lon; Type: CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.climate_location
    ADD CONSTRAINT unique_lat_lon UNIQUE (latitude, longitude);


--
-- Name: idx_device_id; Type: INDEX; Schema: public; Owner: vivarium
--

CREATE INDEX idx_device_id ON public.devices USING btree (device_id);


--
-- Name: idx_device_status_device_id; Type: INDEX; Schema: public; Owner: vivarium
--

CREATE INDEX idx_device_status_device_id ON public.device_status USING btree (device_id);


--
-- Name: idx_device_status_timestamp; Type: INDEX; Schema: public; Owner: vivarium
--

CREATE INDEX idx_device_status_timestamp ON public.device_status USING btree ("timestamp");


--
-- Name: idx_raw_data_temperature; Type: INDEX; Schema: public; Owner: vivarium
--

CREATE INDEX idx_raw_data_temperature ON public.sensor_readings USING gin (((raw_data -> 'temperature'::text)));


--
-- Name: idx_sensor_id; Type: INDEX; Schema: public; Owner: vivarium
--

CREATE INDEX idx_sensor_id ON public.sensors USING btree (sensor_id);


--
-- Name: idx_sensor_readings_sensor_id; Type: INDEX; Schema: public; Owner: vivarium
--

CREATE INDEX idx_sensor_readings_sensor_id ON public.sensor_readings USING btree (sensor_id);


--
-- Name: idx_sensor_readings_timestamp; Type: INDEX; Schema: public; Owner: vivarium
--

CREATE INDEX idx_sensor_readings_timestamp ON public.sensor_readings USING btree ("timestamp");


--
-- Name: climate_astro_data climate_astro_data_location_id_forecast_date_fkey; Type: FK CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.climate_astro_data
    ADD CONSTRAINT climate_astro_data_location_id_forecast_date_fkey FOREIGN KEY (location_id, forecast_date) REFERENCES public.climate_forecast_day(location_id, forecast_date);


--
-- Name: climate_day_data climate_day_data_condition_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.climate_day_data
    ADD CONSTRAINT climate_day_data_condition_code_fkey FOREIGN KEY (condition_code) REFERENCES public.climate_condition(condition_code);


--
-- Name: climate_day_data climate_day_data_location_id_forecast_date_fkey; Type: FK CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.climate_day_data
    ADD CONSTRAINT climate_day_data_location_id_forecast_date_fkey FOREIGN KEY (location_id, forecast_date) REFERENCES public.climate_forecast_day(location_id, forecast_date);


--
-- Name: climate_forecast_day climate_forecast_day_location_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.climate_forecast_day
    ADD CONSTRAINT climate_forecast_day_location_id_fkey FOREIGN KEY (location_id) REFERENCES public.climate_location(location_id);


--
-- Name: climate_hour_data climate_hour_data_condition_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.climate_hour_data
    ADD CONSTRAINT climate_hour_data_condition_code_fkey FOREIGN KEY (condition_code) REFERENCES public.climate_condition(condition_code);


--
-- Name: climate_hour_data climate_hour_data_location_id_forecast_date_fkey; Type: FK CONSTRAINT; Schema: public; Owner: vivarium
--

ALTER TABLE ONLY public.climate_hour_data
    ADD CONSTRAINT climate_hour_data_location_id_forecast_date_fkey FOREIGN KEY (location_id, forecast_date) REFERENCES public.climate_forecast_day(location_id, forecast_date);


--
-- PostgreSQL database dump complete
--

