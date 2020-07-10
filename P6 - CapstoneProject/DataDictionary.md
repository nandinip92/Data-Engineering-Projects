#### Fact Table
It contains the details of the immigrants
  *  cicid : Its serial number given to  each record insertted
  *  i94yr : Year of Immigration
  *  i94mon : Month
  *  i94cit : City from which the candidate immigrated-Code
  *  i94res : Residence of the immigrant -Code
  *  i94port : Port Code
  *  arrival_date :Arrival date to US
  *  i94mode : Mode of immigration
  *  i94addr : Address or State where the Immgrant is going to stay
  *  departure_date :Departure from US
  *  i94visa : Visa purpose Business/Student/Pleasure
  *  biryear : BirthYear
  *  gender :Gender of the person
  *  visatype : Type of visa, different categories under purpose of visit

```script
IMMIGRANTS : Fact Table
 |
 |-- cicid: long (nullable = true)
 |-- i94yr: integer (nullable = true)
 |-- i94mon: integer (nullable = true)
 |-- i94cit: integer (nullable = true)
 |-- i94res: integer (nullable = true)
 |-- i94port: string (nullable = true)
 |-- arrival_date: date (nullable = true)
 |-- i94mode: integer (nullable = true)
 |-- i94addr: string (nullable = true)
 |-- departure_date: date (nullable = true)
 |-- i94visa: integer (nullable = true)
 |-- biryear: integer (nullable = true)
 |-- gender: string (nullable = true)
 |-- visatype: string (nullable = true)
```

#### Dimension Tables

1. MODES  : Mode of immigration table - air, sea, land
2. STATES : Codes of the states
3. CITIES : Codes of cities
4. PORTS : All ports codes for the modes
5. VISAS : Purpose of the visit codes
6. US_CITY_DEMOGRAPHICS : Demographs of the U.S Cities
7. AIRPORTS : All the available airport codes around the world
8. TEMPERATURES : Global TEMPERATURES of all the cities

```sctipt
MODES
 |-- mode_id: integer (nullable = true)
 |-- mode_name: string (nullable = true)

STATES
 |-- state_code: string (nullable = true)
 |-- state: string (nullable = true)

CITIES
 |-- city_code: integer (nullable = true)
 |-- city: string (nullable = true)

PORTS
 |-- port_code: string (nullable = true)
 |-- port_name: string (nullable = true)

VISAS
 |-- visa_id: integer (nullable = true)
 |-- visa_name: string (nullable = true)


 US_CITY_DEMOGRAPHICS
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- median_age: double (nullable = true)
 |-- male_population: integer (nullable = true)
 |-- female_population: integer (nullable = true)
 |-- total_population: integer (nullable = true)
 |-- veterans_count: integer (nullable = true)
 |-- foriegn_born: integer (nullable = true)
 |-- Race: string (nullable = true)
 |-- state_code: string (nullable = true)


AIRPORTS
 |-- ident: string (nullable = true)
 |-- type: string (nullable = true)
 |-- name: string (nullable = true)
 |-- elevation_ft: integer (nullable = true)
 |-- iso_country: string (nullable = true)
 |-- iso_region: string (nullable = true)
 |-- municipality: string (nullable = true)
 |-- gps_code: string (nullable = true)
 |-- local_code: string (nullable = true)
 |-- coordinates: string (nullable = true)


 TEMPERATURES
 |-- noted_date: date (nullable = true)
 |-- Average_Temperature: double (nullable = true)
 |-- Average_Temperature_Uncertainty: double (nullable = true)
 |-- City: string (nullable = true)
 |-- Country: string (nullable = true)
 |-- Latitude: string (nullable = true)
 |-- Longitude: string (nullable = true)
```
