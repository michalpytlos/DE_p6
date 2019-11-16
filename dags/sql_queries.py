class SqlQueries:
    weather_stations_table_insert = ("""
        INSERT INTO weather_stations
            SELECT *, CONCAT(CONCAT('lat',latitude), CONCAT('long',longitude))
            FROM staging_weather_stations
    """)

    zone_table_insert = ("""
        INSERT INTO zones
            SELECT DISTINCT ws.zone_id, ws.latitude, ws.longitude
                FROM weather_stations ws
                LEFT OUTER JOIN zones z
                    ON ws.zone_id = z.zone_id
                WHERE z.zone_id IS NULL
    """)
