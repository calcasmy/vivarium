DROP TABLE if exists  public.raw_climate_data CASCADE;
CREATE TABLE public.raw_climate_data (
    weather_date DATE PRIMARY KEY,
    raw_data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE if exists public.climate_location CASCADE;
CREATE TABLE public.climate_location (
    location_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    region VARCHAR(255),
    country VARCHAR(255),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    timezone_id VARCHAR(255),
    localtime_epoch BIGINT,
    "localtime" VARCHAR(20),
    CONSTRAINT unique_lat_lon UNIQUE (latitude, longitude)
);

DROP TABLE if exists public.climate_forecast_day CASCADE;
CREATE TABLE public.climate_forecast_day (
    location_id INTEGER,
    forecast_date DATE,
    forecast_date_epoch BIGINT,
    PRIMARY KEY (location_id, forecast_date),
    FOREIGN KEY (location_id) REFERENCES public.climate_location(location_id)
);

DROP TABLE if exists public.climate_astro_data CASCADE;
CREATE TABLE public.climate_astro_data (
    location_id INTEGER,
    forecast_date DATE,
    sunrise VARCHAR(10),
    sunset VARCHAR(10),
    moonrise VARCHAR(20),
    moonset VARCHAR(20),
    moon_phase VARCHAR(255),
    moon_illumination INTEGER,
    PRIMARY KEY (location_id, forecast_date),
    FOREIGN KEY (location_id, forecast_date) REFERENCES public.climate_forecast_day(location_id, forecast_date)
);

DROP TABLE if exists public.climate_condition CASCADE;
CREATE TABLE public.climate_condition (
    condition_code INTEGER PRIMARY KEY,
    text VARCHAR(255),
    icon VARCHAR(255)
);

DROP TABLE if exists public.climate_day_data CASCADE;
CREATE TABLE public.climate_day_data (
    location_id INTEGER,
    forecast_date DATE,
    maxtemp_c DECIMAL(5, 2),
    maxtemp_f DECIMAL(5, 2),
    mintemp_c DECIMAL(5, 2),
    mintemp_f DECIMAL(5, 2),
    avgtemp_c DECIMAL(5, 2),
    avgtemp_f DECIMAL(5, 2),
    maxwind_mph DECIMAL(5, 2),
    maxwind_kph DECIMAL(5, 2),
    totalprecip_mm DECIMAL(5, 2),
    totalprecip_in DECIMAL(5, 2),
    totalsnow_cm DECIMAL(5, 2),
    avgvis_km DECIMAL(5, 2),
    avgvis_miles DECIMAL(5, 2),
    avghumidity INTEGER,
    daily_will_it_rain INTEGER,
    daily_chance_of_rain INTEGER,
    daily_will_it_snow INTEGER,
    daily_chance_of_snow INTEGER,
    condition_code INTEGER,
    uv DECIMAL(3, 1),
    PRIMARY KEY (location_id, forecast_date),
    FOREIGN KEY (location_id, forecast_date) REFERENCES public.climate_forecast_day(location_id, forecast_date),
    FOREIGN KEY (condition_code) REFERENCES public.climate_condition(condition_code)
);

DROP TABLE if exists public.climate_hour_data CASCADE;
CREATE TABLE public.climate_hour_data (
    location_id INTEGER,
    forecast_date DATE,
    time_epoch BIGINT,
    time VARCHAR(20),
    temp_c DECIMAL(5, 2),
    temp_f DECIMAL(5, 2),
    is_day INTEGER,
    condition_code INTEGER,
    wind_mph DECIMAL(5, 2),
    wind_kph DECIMAL(5, 2),
    wind_degree INTEGER,
    wind_dir VARCHAR(3),
    pressure_mb DECIMAL(7, 2),
    pressure_in DECIMAL(7, 2),
    precip_mm DECIMAL(5, 2),
    precip_in DECIMAL(5, 2),
    snow_cm DECIMAL(5, 2),
    humidity INTEGER,
    cloud INTEGER,
    feelslike_c DECIMAL(5, 2),
    feelslike_f DECIMAL(5, 2),
    windchill_c DECIMAL(5, 2),
    windchill_f DECIMAL(5, 2),
    heatindex_c DECIMAL(5, 2),
    heatindex_f DECIMAL(5, 2),
    dewpoint_c DECIMAL(5, 2),
    dewpoint_f DECIMAL(5, 2),
    will_it_rain INTEGER,
    chance_of_rain INTEGER,
    will_it_snow INTEGER,
    chance_of_snow INTEGER,
    vis_km DECIMAL(5, 2),
    vis_miles DECIMAL(5, 2),
    gust_mph DECIMAL(5, 2),
    gust_kph DECIMAL(5, 2),
    uv DECIMAL(3, 1),
    PRIMARY KEY (location_id, forecast_date, time_epoch),
    FOREIGN KEY (location_id, forecast_date) REFERENCES public.climate_forecast_day(location_id, forecast_date),
    FOREIGN KEY (condition_code) REFERENCES public.climate_condition(condition_code),
    CONSTRAINT unique_hour_data UNIQUE (location_id, forecast_date, time_epoch)
);