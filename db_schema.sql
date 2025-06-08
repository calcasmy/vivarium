--
-- PostgreSQL database dump
--

-- Dumped from database version 15.13 (Debian 15.13-0+deb12u1)
-- Dumped by pg_dump version 17.4 (Postgres.app)

-- Started on 2025-06-07 19:25:05 EDT

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 214 (class 1259 OID 16675)
-- Name: climate_astro_data; Type: TABLE; Schema: public; Owner: calcasmy
--
DROP TABLE IF EXISTS public.climate_astro_data CASCADE;

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


ALTER TABLE public.climate_astro_data OWNER TO calcasmy;

--
-- TOC entry 215 (class 1259 OID 16678)
-- Name: climate_condition; Type: TABLE; Schema: public; Owner: calcasmy
--

DROP TABLE IF EXISTS public.climate_condition CASCADE;

CREATE TABLE public.climate_condition (
    condition_code integer NOT NULL,
    text character varying(255),
    icon character varying(255)
);


ALTER TABLE public.climate_condition OWNER TO calcasmy;

--
-- TOC entry 216 (class 1259 OID 16683)
-- Name: climate_day_data; Type: TABLE; Schema: public; Owner: calcasmy
--

DROP TABLE IF EXISTS public.climate_day_data CASCADE;

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


ALTER TABLE public.climate_day_data OWNER TO calcasmy;

--
-- TOC entry 217 (class 1259 OID 16686)
-- Name: climate_forecast_day; Type: TABLE; Schema: public; Owner: calcasmy
--

DROP TABLE IF EXISTS public.climate_forecast_day CASCADE;

CREATE TABLE public.climate_forecast_day (
    location_id integer NOT NULL,
    forecast_date date NOT NULL,
    forecast_date_epoch bigint
);


ALTER TABLE public.climate_forecast_day OWNER TO calcasmy;

--
-- TOC entry 218 (class 1259 OID 16689)
-- Name: climate_hour_data; Type: TABLE; Schema: public; Owner: calcasmy
--

DROP TABLE IF EXISTS public.climate_hour_data CASCADE;

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


ALTER TABLE public.climate_hour_data OWNER TO calcasmy;

--
-- TOC entry 219 (class 1259 OID 16692)
-- Name: climate_location; Type: TABLE; Schema: public; Owner: calcasmy
--

DROP TABLE IF EXISTS public.climate_location CASCADE;

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


ALTER TABLE public.climate_location OWNER TO calcasmy;

--
-- TOC entry 220 (class 1259 OID 16697)
-- Name: climate_location_location_id_seq; Type: SEQUENCE; Schema: public; Owner: calcasmy
--

CREATE SEQUENCE public.climate_location_location_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.climate_location_location_id_seq OWNER TO calcasmy;

--
-- TOC entry 3454 (class 0 OID 0)
-- Dependencies: 220
-- Name: climate_location_location_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: calcasmy
--

ALTER SEQUENCE public.climate_location_location_id_seq OWNED BY public.climate_location.location_id;


--
-- TOC entry 221 (class 1259 OID 16698)
-- Name: device_status; Type: TABLE; Schema: public; Owner: calcasmy
--

DROP TABLE IF EXISTS public.device_status CASCADE;

CREATE TABLE public.device_status (
    status_id bigint NOT NULL,
    device_id integer NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    is_on boolean NOT NULL,
    device_data jsonb
);


ALTER TABLE public.device_status OWNER TO calcasmy;

--
-- TOC entry 222 (class 1259 OID 16703)
-- Name: device_status_status_id_seq; Type: SEQUENCE; Schema: public; Owner: calcasmy
--

CREATE SEQUENCE public.device_status_status_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.device_status_status_id_seq OWNER TO calcasmy;

--
-- TOC entry 3455 (class 0 OID 0)
-- Dependencies: 222
-- Name: device_status_status_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: calcasmy
--

ALTER SEQUENCE public.device_status_status_id_seq OWNED BY public.device_status.status_id;


--
-- TOC entry 223 (class 1259 OID 16704)
-- Name: devices; Type: TABLE; Schema: public; Owner: calcasmy
--

DROP TABLE IF EXISTS public.devices CASCADE;

CREATE TABLE public.devices (
    device_id integer NOT NULL,
    device_name character varying(255) NOT NULL,
    device_type character varying(50) NOT NULL,
    location character varying(100),
    model character varying(100),
    date_added date
);


ALTER TABLE public.devices OWNER TO calcasmy;

--
-- TOC entry 224 (class 1259 OID 16709)
-- Name: devices_device_id_seq; Type: SEQUENCE; Schema: public; Owner: calcasmy
--

CREATE SEQUENCE public.devices_device_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.devices_device_id_seq OWNER TO calcasmy;

--
-- TOC entry 3456 (class 0 OID 0)
-- Dependencies: 224
-- Name: devices_device_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: calcasmy
--

ALTER SEQUENCE public.devices_device_id_seq OWNED BY public.devices.device_id;


--
-- TOC entry 225 (class 1259 OID 16710)
-- Name: raw_climate_data; Type: TABLE; Schema: public; Owner: calcasmy
--

DROP TABLE IF EXISTS public.raw_climate_data CASCADE;

CREATE TABLE public.raw_climate_data (
    weather_date date NOT NULL,
    raw_data jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.raw_climate_data OWNER TO calcasmy;

--
-- TOC entry 226 (class 1259 OID 16716)
-- Name: sensor_readings; Type: TABLE; Schema: public; Owner: calcasmy
--

DROP TABLE IF EXISTS public.sensor_readings CASCADE;

CREATE TABLE public.sensor_readings (
    reading_id bigint NOT NULL,
    sensor_id integer NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    raw_data jsonb
);


ALTER TABLE public.sensor_readings OWNER TO calcasmy;

--
-- TOC entry 227 (class 1259 OID 16721)
-- Name: sensor_readings_reading_id_seq; Type: SEQUENCE; Schema: public; Owner: calcasmy
--

CREATE SEQUENCE public.sensor_readings_reading_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--
-- TOC entry 3250 (class 2606 OID 17146)
-- Name: climate_astro_data climate_astro_data_pkey; Type: CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.climate_astro_data
    ADD CONSTRAINT climate_astro_data_pkey PRIMARY KEY (location_id, forecast_date);


--
-- TOC entry 3252 (class 2606 OID 17148)
-- Name: climate_condition climate_condition_pkey; Type: CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.climate_condition
    ADD CONSTRAINT climate_condition_pkey PRIMARY KEY (condition_code);


--
-- TOC entry 3254 (class 2606 OID 17150)
-- Name: climate_day_data climate_day_data_pkey; Type: CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.climate_day_data
    ADD CONSTRAINT climate_day_data_pkey PRIMARY KEY (location_id, forecast_date);


--
-- TOC entry 3256 (class 2606 OID 17152)
-- Name: climate_forecast_day climate_forecast_day_pkey; Type: CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.climate_forecast_day
    ADD CONSTRAINT climate_forecast_day_pkey PRIMARY KEY (location_id, forecast_date);


--
-- TOC entry 3260 (class 2606 OID 17154)
-- Name: climate_location climate_location_pkey; Type: CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.climate_location
    ADD CONSTRAINT climate_location_pkey PRIMARY KEY (location_id);


--
-- TOC entry 3264 (class 2606 OID 17156)
-- Name: device_status device_status_pkey; Type: CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.device_status
    ADD CONSTRAINT device_status_pkey PRIMARY KEY (status_id);


--
-- TOC entry 3268 (class 2606 OID 17158)
-- Name: devices devices_device_name_key; Type: CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.devices
    ADD CONSTRAINT devices_device_name_key UNIQUE (device_name);


--
-- TOC entry 3270 (class 2606 OID 17160)
-- Name: devices devices_pkey; Type: CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.devices
    ADD CONSTRAINT devices_pkey PRIMARY KEY (device_id);


--
-- TOC entry 3273 (class 2606 OID 17162)
-- Name: raw_climate_data raw_climate_data_pkey; Type: CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.raw_climate_data
    ADD CONSTRAINT raw_climate_data_pkey PRIMARY KEY (weather_date);


--
-- TOC entry 3278 (class 2606 OID 17164)
-- Name: sensor_readings sensor_readings_pkey; Type: CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.sensor_readings
    ADD CONSTRAINT sensor_readings_pkey PRIMARY KEY (reading_id);


--
-- TOC entry 3281 (class 2606 OID 17166)
-- Name: sensors sensors_pkey; Type: CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.sensors
    ADD CONSTRAINT sensors_pkey PRIMARY KEY (sensor_id);


--
-- TOC entry 3283 (class 2606 OID 17168)
-- Name: sensors sensors_sensor_name_key; Type: CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.sensors
    ADD CONSTRAINT sensors_sensor_name_key UNIQUE (sensor_name);


--
-- TOC entry 3258 (class 2606 OID 17170)
-- Name: climate_hour_data unique_hour_data; Type: CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.climate_hour_data
    ADD CONSTRAINT unique_hour_data PRIMARY KEY (location_id, forecast_date, time_epoch);


--
-- TOC entry 3262 (class 2606 OID 17172)
-- Name: climate_location unique_lat_lon; Type: CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.climate_location
    ADD CONSTRAINT unique_lat_lon UNIQUE (latitude, longitude);


--
-- TOC entry 3271 (class 1259 OID 17173)
-- Name: idx_device_id; Type: INDEX; Schema: public; Owner: calcasmy
--

CREATE INDEX idx_device_id ON public.devices USING btree (device_id);


--
-- TOC entry 3265 (class 1259 OID 17174)
-- Name: idx_device_status_device_id; Type: INDEX; Schema: public; Owner: calcasmy
--

CREATE INDEX idx_device_status_device_id ON public.device_status USING btree (device_id);


--
-- TOC entry 3266 (class 1259 OID 17175)
-- Name: idx_device_status_timestamp; Type: INDEX; Schema: public; Owner: calcasmy
--

CREATE INDEX idx_device_status_timestamp ON public.device_status USING btree ("timestamp");


--
-- TOC entry 3274 (class 1259 OID 17176)
-- Name: idx_raw_data_temperature; Type: INDEX; Schema: public; Owner: calcasmy
--

CREATE INDEX idx_raw_data_temperature ON public.sensor_readings USING gin (((raw_data -> 'temperature'::text)));


--
-- TOC entry 3279 (class 1259 OID 17177)
-- Name: idx_sensor_id; Type: INDEX; Schema: public; Owner: calcasmy
--

CREATE INDEX idx_sensor_id ON public.sensors USING btree (sensor_id);


--
-- TOC entry 3275 (class 1259 OID 17178)
-- Name: idx_sensor_readings_sensor_id; Type: INDEX; Schema: public; Owner: calcasmy
--

CREATE INDEX idx_sensor_readings_sensor_id ON public.sensor_readings USING btree (sensor_id);


--
-- TOC entry 3276 (class 1259 OID 17179)
-- Name: idx_sensor_readings_timestamp; Type: INDEX; Schema: public; Owner: calcasmy
--

CREATE INDEX idx_sensor_readings_timestamp ON public.sensor_readings USING btree ("timestamp");


--
-- TOC entry 3284 (class 2606 OID 17180)
-- Name: climate_astro_data climate_astro_data_location_id_forecast_date_fkey; Type: FK CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.climate_astro_data
    ADD CONSTRAINT climate_astro_data_location_id_forecast_date_fkey FOREIGN KEY (location_id, forecast_date) REFERENCES public.climate_forecast_day(location_id, forecast_date);


--
-- TOC entry 3285 (class 2606 OID 17185)
-- Name: climate_day_data climate_day_data_condition_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.climate_day_data
    ADD CONSTRAINT climate_day_data_condition_code_fkey FOREIGN KEY (condition_code) REFERENCES public.climate_condition(condition_code);


--
-- TOC entry 3286 (class 2606 OID 17190)
-- Name: climate_day_data climate_day_data_location_id_forecast_date_fkey; Type: FK CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.climate_day_data
    ADD CONSTRAINT climate_day_data_location_id_forecast_date_fkey FOREIGN KEY (location_id, forecast_date) REFERENCES public.climate_forecast_day(location_id, forecast_date);


--
-- TOC entry 3287 (class 2606 OID 17195)
-- Name: climate_forecast_day climate_forecast_day_location_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.climate_forecast_day
    ADD CONSTRAINT climate_forecast_day_location_id_fkey FOREIGN KEY (location_id) REFERENCES public.climate_location(location_id);


--
-- TOC entry 3288 (class 2606 OID 17200)
-- Name: climate_hour_data climate_hour_data_condition_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.climate_hour_data
    ADD CONSTRAINT climate_hour_data_condition_code_fkey FOREIGN KEY (condition_code) REFERENCES public.climate_condition(condition_code);


--
-- TOC entry 3289 (class 2606 OID 17205)
-- Name: climate_hour_data climate_hour_data_location_id_forecast_date_fkey; Type: FK CONSTRAINT; Schema: public; Owner: calcasmy
--

ALTER TABLE ONLY public.climate_hour_data
    ADD CONSTRAINT climate_hour_data_location_id_forecast_date_fkey FOREIGN KEY (location_id, forecast_date) REFERENCES public.climate_forecast_day(location_id, forecast_date);


-- Completed on 2025-06-07 19:25:07 EDT

--
-- PostgreSQL database dump complete
--

