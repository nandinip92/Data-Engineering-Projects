drop_modes_table = """DROP TABLE IF EXISTS MODES CASCADE ;"""
drop_visas_table = """DROP TABLE IF EXISTS VISAS CASCADE ;"""
drop_ports_table = """DROP TABLE IF EXISTS PORTS CASCADE ;"""
drop_state_stable = """DROP TABLE IF EXISTS STATES CASCADE ;"""
drop_cities_table = """DROP TABLE IF EXISTS CITIES CASCADE ; """
drop_airports_table = """DROP TABLE IF EXISTS AIRPORTS CASCADE ; """
drop_us_city_demographics_table = """DROP TABLE IF EXISTS US_CITY_DEMOGRAPHICS CASCADE ; """
drop_temperatues_table = """DROP TABLE IF EXISTS TEMPERATURES CASCADE ; """
drop_immigrants_fact_table = """DROP TABLE IF EXISTS IMMIGRANTS CASCADE ; """


modes_table = """CREATE TABLE IF NOT EXISTS MODES (
                      MODE_ID SMALLINT,
                      MODE_NAME VARCHAR,
                      PRIMARY KEY(MODE_ID)
                    ) DISTSTYLE ALL;
                    """

visas_table = """CREATE TABLE IF NOT EXISTS VISAS (
                    VISA_ID SMALLINT,
                    VISA_NAME VARCHAR,
                    PRIMARY KEY(VISA_ID)
                    ) DISTSTYLE ALL;
                """
ports_table = """CREATE TABLE IF NOT EXISTS PORTS (
                    PORT_CODE VARCHAR,
                    PORT_NAME VARCHAR,
                    PRIMARY KEY(PORT_CODE)
                ) DISTSTYLE ALL;
                """

state_stable = """CREATE TABLE IF NOT EXISTS STATES (
                    STATE_CODE VARCHAR,
                    STATE VARCHAR,
                    PRIMARY KEY(STATE_CODE)
                ) DISTSTYLE ALL;
                """

cities_table = """CREATE TABLE IF NOT EXISTS CITIES (
                        CITY_CODE VARCHAR,
                        CITY VARCHAR,
                        PRIMARY KEY(CITY_CODE)
                      ) DISTSTYLE ALL;
                    """

airports_table = """CREATE TABLE IF NOT EXISTS AIRPORTS (
                          IDENT VARCHAR,
                          TYPE VARCHAR,
                          NAME VARCHAR,
                          ELEVATION_FT INTEGER,
                          ISO_COUNTRY VARCHAR,
                          ISO_REGION VARCHAR,
                          MUNICIPALITY VARCHAR,
                          GPS_CODE VARCHAR,
                          LOCAL_CODE VARCHAR REFERENCES PORTS(PORT_CODE),
                          COORDINATES VARCHAR,
                          PRIMARY KEY (IDENT, ISO_COUNTRY, ISO_REGION)
                      )COMPOUND SORTKEY(ISO_COUNTRY, ISO_REGION);
                    """


temperatues_table = """CREATE TABLE IF NOT EXISTS TEMPERATURES (
                          NOTED_DATE DATE,
                          AVERAGE_TEMPERATURE DOUBLE PRECISION  ,
                          AVERAGE_TEMPERATURE_UNCERTAINTY DOUBLE PRECISION ,
                          CITY VARCHAR REFERENCES US_CITY_DEMOGRAPHICS(CITY),
                          COUNTRY VARCHAR,
                          LATITUDE VARCHAR,
                          LONGITUDE VARCHAR,
                          PRIMARY KEY (NOTED_DATE,CITY,COUNTRY)
                        )COMPOUND SORTKEY(CITY,COUNTRY);
                        """

us_city_demographics_table = """CREATE TABLE IF NOT EXISTS US_CITY_DEMOGRAPHICS (
                                    CITY VARCHAR,
                                    STATE VARCHAR,
                                    MEDIAN_AGE INTEGER,
                                    MALE_POPULATION INTEGER,
                                    FEMALE_POPULATION INTEGER,
                                    TOTAL_POPULATION INTEGER,
                                    VETERANS_COUNT INTEGER,
                                    FOREIGN_BORN INTEGER,
                                    RACE VARCHAR,
                                    STATE_CODE VARCHAR REFERENCES STATES(STATE_CODE),
                                    PRIMARY KEY(CITY)
                                    ) COMPOUND SORTKEY(CITY, STATE_CODE);
                                    """


immigrants_fact_table = """CREATE TABLE IF NOT EXISTS IMMIGRANTS (
                                CICID BIGINT,
                                I94YR INTEGER,
                                I94MON SMALLINT,
                                I94CIT INTEGER REFERENCES CITIES(CITY_CODE),
                                I94RES INTEGER REFERENCES CITIES(CITY_CODE),
                                I94PORT VARCHAR REFERENCES PORTS(PORT_CODE),
                                ARRIVAL_DATE DATE,
                                i94mode SMALLINT REFERENCES MODES(MODE_ID),
                                I94ADDR VARCHAR REFERENCES STATES (STATE_CODE),
                                DEPARTURE_DATE DATE,
                                I94VISA SMALLINT REFERENCES VISAS(VISA_ID),
                                BIRYEAR INTEGER,
                                GENDER VARCHAR,
                                VISATYPE VARCHAR,
                                PRIMARY KEY(CICID)
                              ) COMPOUND SORTKEY(I94YR,I94MON,I94CIT,I94RES);
                            """


create_table_queries = [
    modes_table,
    visas_table,
    ports_table,
    state_stable,
    cities_table,
    airports_table,
    us_city_demographics_table,
    temperatues_table,
    immigrants_fact_table,
]
drop_table_queries = [
    drop_immigrants_fact_table,
    drop_us_city_demographics_table,
    drop_temperatues_table,
    drop_airports_table,
    drop_modes_table,
    drop_visas_table,
    drop_ports_table,
    drop_state_stable,
    drop_cities_table    
]
