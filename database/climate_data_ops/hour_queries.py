# vivarium# vivarium/database/climate_data_ops/hour_queries.py

import psycopg2
from psycopg2 import sql
from typing import Dict, List, Optional, Any, Union

from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations
from database.climate_data_ops.base_query_strategy import BaseQuery
from database.climate_data_ops.condition_queries import ConditionQueries # Corrected import path assuming it's in the same climate_data_ops dir


logger = LogHelper.get_logger(__name__)


class HourQueries(BaseQuery):
    """
    Manages database interactions for hourly forecast data in the 'public.climate_hour_data' table.
    """

    def __init__(self, db_operations: DBOperations):
        """
        Initializes the HourQueries instance.

        :param db_operations: An active DBOperations instance for database connectivity.
        """
        super().__init__(db_operations)
        self.condition_db = ConditionQueries(db_operations) # Initialize ConditionQueries with the same DBOperations instance
        logger.debug("HourQueries initialized.")

    def insert(self, location_id: int, forecast_date: str, hour_data_list: List[Dict[str, Any]]) -> bool:
        """
        Inserts or updates a list of hourly forecast data for a given forecast day.

        Implements the abstract 'insert' method from BaseQuery.
        Iterates through the list, inserting/updating each hourly record.
        Handles insertion of associated condition data.

        :param location_id: The ID of the associated location.
        :param forecast_date: The date (YYYY-MM-DD) of the forecast.
        :param hour_data_list: A list of dictionaries, each containing hourly forecast details.
        :return: True if all operations were successful (no errors), False otherwise.
        """
        query = sql.SQL("""
            INSERT INTO public.climate_hour_data (
                location_id, forecast_date, time_epoch, time, temp_c, temp_f, is_day,
                condition_code, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb,
                pressure_in, precip_mm, precip_in, snow_cm, humidity, cloud, feelslike_c,
                feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c,
                dewpoint_f, will_it_rain, chance_of_rain, will_it_snow, chance_of_snow,
                vis_km, vis_miles, gust_mph, gust_kph, uv
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (location_id, forecast_date, time_epoch) DO UPDATE SET
                time = EXCLUDED.time, temp_c = EXCLUDED.temp_c, temp_f = EXCLUDED.temp_f, is_day = EXCLUDED.is_day,
                condition_code = EXCLUDED.condition_code, wind_mph = EXCLUDED.wind_mph, wind_kph = EXCLUDED.wind_kph,
                wind_degree = EXCLUDED.wind_degree, wind_dir = EXCLUDED.wind_dir, pressure_mb = EXCLUDED.pressure_mb,
                pressure_in = EXCLUDED.pressure_in, precip_mm = EXCLUDED.precip_mm, precip_in = EXCLUDED.precip_in,
                snow_cm = EXCLUDED.snow_cm, humidity = EXCLUDED.humidity, cloud = EXCLUDED.cloud,
                feelslike_c = EXCLUDED.feelslike_c, feelslike_f = EXCLUDED.feelslike_f, windchill_c = EXCLUDED.windchill_c,
                windchill_f = EXCLUDED.windchill_f, heatindex_c = EXCLUDED.heatindex_c, heatindex_f = EXCLUDED.heatindex_f,
                dewpoint_c = EXCLUDED.dewpoint_c, dewpoint_f = EXCLUDED.dewpoint_f, will_it_rain = EXCLUDED.will_it_rain,
                chance_of_rain = EXCLUDED.chance_of_rain, will_it_snow = EXCLUDED.will_it_snow,
                chance_of_snow = EXCLUDED.chance_of_snow, vis_km = EXCLUDED.vis_km, vis_miles = EXCLUDED.vis_miles,
                gust_mph = EXCLUDED.gust_mph, gust_kph = EXCLUDED.gust_kph, uv = EXCLUDED.uv;
        """)

        all_successful = True
        for hour_data in hour_data_list:
            condition_code = hour_data.get('condition', {}).get('code')

            # Insert hourly condition if it doesn't already exist
            if condition_code is not None:
                # The condition_db.insert method already handles ON CONFLICT DO NOTHING
                if not self.condition_db.insert(hour_data.get('condition', {})):
                    # Log a warning if condition insertion fails, but don't stop the main loop
                    # This might happen if there's an error beyond simple ON CONFLICT
                    logger.warning(f"Failed to insert or verify condition for code {condition_code} during hourly data processing.")

            params = (
                location_id,
                forecast_date,
                hour_data.get('time_epoch'), hour_data.get('time'),
                hour_data.get('temp_c'), hour_data.get('temp_f'),
                hour_data.get('is_day'), condition_code,
                hour_data.get('wind_mph'), hour_data.get('wind_kph'),
                hour_data.get('wind_degree'), hour_data.get('wind_dir'),
                hour_data.get('pressure_mb'), hour_data.get('pressure_in'),
                hour_data.get('precip_mm'), hour_data.get('precip_in'),
                hour_data.get('snow_cm'), hour_data.get('humidity'),
                hour_data.get('cloud'), hour_data.get('feelslike_c'),
                hour_data.get('feelslike_f'), hour_data.get('windchill_c'),
                hour_data.get('windchill_f'), hour_data.get('heatindex_c'),
                hour_data.get('heatindex_f'), hour_data.get('dewpoint_c'),
                hour_data.get('dewpoint_f'), hour_data.get('will_it_rain'),
                hour_data.get('chance_of_rain'), hour_data.get('will_it_snow'),
                hour_data.get('chance_of_snow'), hour_data.get('vis_km'),
                hour_data.get('vis_miles'), hour_data.get('gust_mph'),
                hour_data.get('gust_kph'), hour_data.get('uv')
            )
            try:
                self.db_ops.execute_query(query, params, fetch=False)
                logger.debug(f"Hourly data for location ID {location_id}, date '{forecast_date}', time_epoch {hour_data.get('time_epoch')} processed.")
            except psycopg2.Error as e:
                logger.error(f"Database error processing hourly data for location ID {location_id}, date '{forecast_date}', time_epoch {hour_data.get('time_epoch')}: {e}", exc_info=True)
                all_successful = False
            except Exception as e:
                logger.error(f"Unexpected error processing hourly data for location ID {location_id}, date '{forecast_date}', time_epoch {hour_data.get('time_epoch')}: {e}", exc_info=True)
                all_successful = False

        if all_successful:
            logger.info(f"All hourly data for location ID {location_id}, date '{forecast_date}' processed successfully.")
        else:
            logger.warning(f"Some hourly data operations failed for location ID {location_id}, date '{forecast_date}'. Check logs for details.")
        return all_successful

    def get(self, location_id: int, forecast_date: str, time_epoch: int) -> Optional[Dict[str, Any]]:
        """
        Retrieves a single hourly forecast record from 'public.climate_hour_data'
        by location ID, forecast date, and time epoch.

        Implements the abstract 'get' method from BaseQuery.

        :param location_id: The ID of the associated location.
        :param forecast_date: The date (YYYY-MM-DD) of the forecast.
        :param time_epoch: The epoch timestamp of the specific hour.
        :return: A dictionary containing the hourly forecast data if found, None otherwise.
        """
        query = sql.SQL("""
            SELECT
                location_id, forecast_date, time_epoch, time, temp_c, temp_f, is_day,
                condition_code, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb,
                pressure_in, precip_mm, precip_in, snow_cm, humidity, cloud, feelslike_c,
                feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c,
                dewpoint_f, will_it_rain, chance_of_rain, will_it_snow,
                chance_of_snow, vis_km, vis_miles, gust_mph, gust_kph, uv
            FROM public.climate_hour_data
            WHERE location_id = %s AND forecast_date = %s AND time_epoch = %s;
        """)
        params = (location_id, forecast_date, time_epoch)
        try:
            result = self.db_ops.execute_query(query, params, fetch_one=True)
            if result:
                logger.info(f"Hourly data found for location ID {location_id}, date '{forecast_date}', time_epoch {time_epoch}.")
                return result
            logger.info(f"No hourly data found for location ID {location_id}, date '{forecast_date}', time_epoch {time_epoch}.")
            return None
        except psycopg2.Error as e:
            logger.error(f"Database error retrieving hourly data for location ID {location_id}, date '{forecast_date}', time_epoch {time_epoch}: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error retrieving hourly data for location ID {location_id}, date '{forecast_date}', time_epoch {time_epoch}: {e}", exc_info=True)
            return None

    def update(self, location_id: int, forecast_date: str, time_epoch: int, new_data: Dict[str, Any]) -> bool:
        """
        Updates an existing hourly forecast record in 'public.climate_hour_data'.

        Note: This method assumes `DBOperations.execute_query` returns True for successful
        DML operations or raises an exception on failure.

        Implements the abstract 'update' method from BaseQuery.

        :param location_id: The ID of the location associated with the forecast.
        :param forecast_date: The date (YYYY-MM-DD) of the forecast.
        :param time_epoch: The epoch timestamp of the specific hour to update.
        :param new_data: Dictionary containing the new values for hourly forecast attributes.
        :return: True if the update was successful (no error), False otherwise.
        """
        set_clauses = []
        params = []

        updatable_fields = [
            'time', 'temp_c', 'temp_f', 'is_day', 'condition_code', 'wind_mph', 'wind_kph',
            'wind_degree', 'wind_dir', 'pressure_mb', 'pressure_in', 'precip_mm', 'precip_in',
            'snow_cm', 'humidity', 'cloud', 'feelslike_c', 'feelslike_f', 'windchill_c',
            'windchill_f', 'heatindex_c', 'heatindex_f', 'dewpoint_c', 'dewpoint_f',
            'will_it_rain', 'chance_of_rain', 'will_it_snow', 'chance_of_snow',
            'vis_km', 'vis_miles', 'gust_mph', 'gust_kph', 'uv'
        ]

        # Handle condition_code potentially nested in 'condition'
        if 'condition' in new_data and 'code' in new_data['condition']:
            set_clauses.append(sql.Identifier('condition_code') + sql.SQL(' = %s'))
            params.append(new_data['condition']['code'])
        elif 'condition_code' in new_data: # If already flattened
            set_clauses.append(sql.Identifier('condition_code') + sql.SQL(' = %s'))
            params.append(new_data['condition_code'])

        for field in updatable_fields:
            if field in new_data:
                set_clauses.append(sql.Identifier(field) + sql.SQL(' = %s'))
                params.append(new_data[field])

        if not set_clauses:
            logger.warning(f"No valid fields provided to update for hourly data (location ID {location_id}, date {forecast_date}, time_epoch {time_epoch}).")
            return False

        query = sql.SQL("""
            UPDATE public.climate_hour_data
            SET {}
            WHERE location_id = %s AND forecast_date = %s AND time_epoch = %s;
        """).format(sql.SQL(', ').join(set_clauses))
        params.extend([location_id, forecast_date, time_epoch])

        try:
            self.db_ops.execute_query(query, tuple(params), fetch=False)
            logger.info(f"Hourly data for location ID {location_id}, date '{forecast_date}', time_epoch {time_epoch} updated. Verification of affected rows may be needed externally.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Database error updating hourly data for location ID {location_id}, date '{forecast_date}', time_epoch {time_epoch}: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error updating hourly data for location ID {location_id}, date '{forecast_date}', time_epoch {time_epoch}: {e}", exc_info=True)
            return False

    def delete(self, location_id: int, forecast_date: str, time_epoch: int) -> bool:
        """
        Deletes an hourly forecast record from 'public.climate_hour_data'
        by location ID, forecast date, and time epoch.

        Note: This method assumes `DBOperations.execute_query` returns True for successful
        DML operations or raises an exception on failure.

        Implements the abstract 'delete' method from BaseQuery.

        :param location_id: The ID of the location associated with the forecast.
        :param forecast_date: The date (YYYY-MM-DD) of the forecast.
        :param time_epoch: The epoch timestamp of the specific hour to delete.
        :return: True if the deletion was successful (no error), False otherwise.
        """
        query = sql.SQL("""
            DELETE FROM public.climate_hour_data
            WHERE location_id = %s AND forecast_date = %s AND time_epoch = %s;
        """)
        params = (location_id, forecast_date, time_epoch)

        try:
            self.db_ops.execute_query(query, params, fetch=False)
            logger.info(f"Hourly data for location ID {location_id}, date '{forecast_date}', time_epoch {time_epoch} deleted. Verification of affected rows may be needed externally.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Database error deleting hourly data for location ID {location_id}, date '{forecast_date}', time_epoch {time_epoch}: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error deleting hourly data for location ID {location_id}, date '{forecast_date}', time_epoch {time_epoch}: {e}", exc_info=True)
            return False

    def get_hourly_data_by_forecast_day(self, location_id: int, forecast_date: str) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieves all hourly forecast data for a given location ID and forecast date.

        :param location_id: The ID of the associated location.
        :param forecast_date: The date (YYYY-MM-DD) of the forecast.
        :return: A list of dictionaries, each containing hourly forecast data, or None if no data is found or on error.
        """
        query = sql.SQL("""
            SELECT
                location_id, forecast_date, time_epoch, time, temp_c, temp_f, is_day,
                condition_code, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb,
                pressure_in, precip_mm, precip_in, snow_cm, humidity, cloud, feelslike_c,
                feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c,
                dewpoint_f, will_it_rain, chance_of_rain, will_it_snow,
                chance_of_snow, vis_km, vis_miles, gust_mph, gust_kph, uv
            FROM public.climate_hour_data
            WHERE location_id = %s AND forecast_date = %s
            ORDER BY time_epoch;
        """)
        params = (location_id, forecast_date,)
        try:
            result = self.db_ops.execute_query(query, params, fetch=True)
            if result:
                logger.info(f"All hourly data found for location ID {location_id}, date '{forecast_date}'.")
                return result
            logger.info(f"No hourly data found for location ID {location_id}, date '{forecast_date}'.")
            return None
        except psycopg2.Error as e:
            logger.error(f"Database error retrieving all hourly data for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error retrieving all hourly data for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return None

    def fetch_time_epochs_for_day(self, location_id: int, forecast_date: str) -> Optional[List[Dict[str, int]]]:
        """
        Fetches all time_epoch values from 'public.climate_hour_data' table
        for a given location_id and forecast_date.

        :param location_id: The location ID.
        :param forecast_date: The forecast date (e.g., '2024-07-28').
        :return: A list of dictionaries, where each dictionary contains {'time_epoch': value},
                 or None on error or if no data is found.
        """
        query = sql.SQL("""
            SELECT time_epoch
            FROM public.climate_hour_data
            WHERE location_id = %s AND forecast_date = %s
            ORDER BY time_epoch;
        """)
        params = (location_id, forecast_date,)
        try:
            result = self.db_ops.execute_query(query, params, fetch=True)
            if result:
                logger.info(f"Time epochs found for location ID {location_id}, date '{forecast_date}'.")
                return result
            logger.info(f"No time epochs found for location ID {location_id}, date '{forecast_date}'.")
            return None
        except psycopg2.Error as e:
            logger.error(f"Database error fetching time epochs for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching time epochs for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return None