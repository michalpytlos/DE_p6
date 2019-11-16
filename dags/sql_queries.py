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

    weather_table_insert = ("""
        INSERT INTO weather
            SELECT
                sw.station_id,
                to_timestamp(sw.date, 'YYYYMMDD'),
                sw.element,
                sw.value,
                ws.zone_id,
                sw.m_flag,
                sw.q_flag,
                sw.s_flag
                FROM staging_weather sw
                JOIN weather_stations ws
                    ON sw.station_id = ws.station_id
    """)

    time_table_insert = ("""
        INSERT INTO time
            SELECT
                DISTINCT w.ts,
                EXTRACT(h from w.ts),
                EXTRACT(d from w.ts),
                EXTRACT(w from w.ts),
                EXTRACT(mon from w.ts),
                EXTRACT(y from w.ts),
                EXTRACT(weekday from w.ts)
                FROM weather w
                LEFT OUTER JOIN time
                    ON w.ts = time.ts
                WHERE time.ts IS NULL
    """)

    zone_table_quality_check = ("""
        SELECT COUNT(*)
            FROM zones
            WHERE latitude < -90
                OR latitude > 90
                OR longitude < -180
                OR longitude > 180
    """)

    air_quality_table_quality_check = ("""
        SELECT COUNT(*)
            FROM air_quality
            WHERE parameter NOT IN ('pm10', 'pm25', 'o3', 'so2', 'co', 'no2')
    """)
