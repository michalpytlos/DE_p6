class SqlQueries:
    weather_stations_table_insert = ("""
        INSERT INTO weather_stations
            SELECT *, CONCAT(CONCAT('lat',latitude), CONCAT('long',longitude))
            FROM staging_weather_stations
    """)
