-- SQL statements for PostgreSQL database

-- Sensors Table
DROP TABLE IF EXISTS public.sensors CASCADE;
CREATE TABLE public.sensors (
    sensor_id SERIAL PRIMARY KEY,
    sensor_name VARCHAR(255) NOT NULL UNIQUE,
    sensor_type VARCHAR(50) NOT NULL,
    location VARCHAR(100),
    model VARCHAR(100),
    date_installed DATE
);

-- Index on sensor_id (although it's already indexed as the primary key, explicitly adding it again won't hurt and can sometimes be beneficial for query optimization in specific scenarios)
CREATE INDEX idx_sensor_id ON sensors (sensor_id);


-- SensorReadings Table
DROP TABLE IF EXISTS public.sensor_readings CASCADE;
CREATE TABLE public.sensor_readings (
    reading_id BIGSERIAL PRIMARY KEY,
    sensor_id INT NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    raw_data JSONB,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Index on sensor_id for efficient querying of readings by sensor
CREATE INDEX idx_sensor_readings_sensor_id ON sensor_readings (sensor_id);

-- Index on timestamp for efficient querying of readings by time
CREATE INDEX idx_sensor_readings_timestamp ON sensor_readings (timestamp);

CREATE INDEX idx_raw_data_temperature ON sensor_readings USING gin ((raw_data -> 'temperature'));


-- Devices Table
DROP TABLE IF EXISTS public.devices CASCADE;
CREATE TABLE public.devices (
    device_id SERIAL PRIMARY KEY,
    device_name VARCHAR(255) NOT NULL UNIQUE,
    device_type VARCHAR(50) NOT NULL,
    location VARCHAR(100),
    model VARCHAR(100),
    date_added DATE
);

-- Index on device_id (similar to sensors, it's the primary key but explicitly adding can be useful)
CREATE INDEX idx_device_id ON devices (device_id);


-- DeviceStatus Table
DROP TABLE IF EXISTS public.device_status CASCADE;
CREATE TABLE public.device_status (
    status_id BIGSERIAL PRIMARY KEY,
    device_id INT NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    is_on BOOLEAN NOT NULL,
    device_data JSONB,
    FOREIGN KEY (device_id) REFERENCES devices(device_id)
);

-- Index on device_id for efficient querying of status by device
CREATE INDEX idx_device_status_device_id ON device_status (device_id);

-- Index on timestamp for efficient querying of status changes over time
CREATE INDEX idx_device_status_timestamp ON device_status (timestamp);


INSERT INTO public.sensors (sensor_name, sensor_type, location, model, date_installed)
VALUES ('Adafruit HTU21D-F', 'Temperature/Humidity Sensor', 'terrarium', 'Adafruit HTU21D-F', '2024-04-01');