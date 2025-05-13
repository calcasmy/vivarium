## Description

`vivarium-weather` is a Python package designed to retrieve and store daily weather information. It fetches raw weather data from an external API, parses the relevant information, and stores it in a PostgreSQL database.  This package is designed to automate the process of collecting and managing weather data for a specific location.

## Features

* **Data Retrieval:** Fetches historical weather data from a weather API.
* **Data Storage:** Stores raw and parsed weather data in a PostgreSQL database.
* **Location Management:** Handles location data, ensuring unique storage of location information.
* **Forecast Processing:** Processes and stores daily forecast data, including day and hour details.
* **Astro Data:** Captures and stores astronomical data (sunrise, sunset, etc.).
* **Modular Design:** Organized into sub-packages for `atmosphere` (API interaction), `database` (database operations), and `utilities` (logging and configuration).
* **Configuration:** Uses a `config.ini` file for database connection and logging settings.
* **Logging:** Implements detailed logging using Python's `logging` module.
* **Installable Package:** Can be easily installed using `pip`.
* **Command-line Interface:** Provides a `vivarium-fetch` command to run the data fetching process.

## Requirements

* Python 3.7 or later
* PostgreSQL database
* `requests` library
* Other dependencies as specified in `pyproject.toml`

## Installation

1.  **Clone the repository:**

    ```bash
    git clone [https://github.com/yourusername/vivarium-weather.git](https://github.com/yourusername/vivarium-weather.git)  # Replace
    cd vivarium-weather
    ```

2.  **Create a virtual environment (recommended):**

    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Linux/macOS
    venv\Scripts\activate  # On Windows
    ```

3.  **Install the package:**

    ```bash
    pip install .
    ```

## Usage

1.  **Configure the database:**

    * Create a `config.ini` file in the project root directory.  See `config.ini.example` for the format.  You will need to provide your PostgreSQL database credentials.

2.  **Run the data fetching script:**

    ```bash
    vivarium-fetch
    ```

    * This command will fetch and store the weather data for the previous day.

## Database Schema

The application uses the following PostgreSQL database schema:

* **public.climate_location:** Stores location information (location\_id, name, region, country, latitude, longitude, timezone\_id, localtime\_epoch, localtime).
* **public.raw\_data:** Stores the raw JSON response from the weather API (raw\_data\_id, date, raw\_json).
* **public.climate\_forecast\_day:** Stores forecast data for a specific day (forecast\_id, location\_id, forecast\_date, forecast\_date\_epoch).
* **public.climate\_day:** Stores daily summary data (day\_id, location\_id, forecast\_date, max\_temp\_c, min\_temp\_c, avg\_temp\_c, max\_wind\_kph, total\_precip\_mm, avg\_humidity, daily\_will\_it\_rain, daily\_chance\_of\_rain, daily\_will\_it\_snow, daily\_chance\_of\_snow).
* **public.climate\_astro:** Stores astronomical data (astro\_id, location\_id, forecast\_date, sunrise, sunset, moonrise, moonset, moon\_phase).
* **public.climate\_condition:** Stores weather condition descriptions (condition\_id, text, icon, code).
* **public.climate\_hour:** Stores hourly forecast data (hour\_id, location\_id, forecast\_date, time, temp\_c, wind\_kph, wind\_dir, precip\_mm, humidity, cloud, will\_it\_rain, chance\_of\_rain, will\_it\_snow, chance\_of\_snow, condition\_id).

## Contributing

1.  Fork the repository.
2.  Create a new branch for your feature or bug fix.
3.  Commit your changes.
4.  Push to your fork.
5.  Submit a pull request.

## License

[MIT License](LICENSE)  # Or your chosen license

## Author

Your Name  
Your Email
