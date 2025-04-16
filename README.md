# Restaurant & Weather Data Processing with Spark

This is my implementation of the restaurant/weather data processing project using Apache Spark. The main goal was to join datasets based on geographic location, filter invalid records, and save the results in a partitioned format.

## What the Project Does

The application:
1. Loads restaurant and weather data from CSV files
2. Filters out records with missing lat/lng coordinates
3. Generates geohashes for both datasets (uses 4 chars for better matching)
4. Joins the datasets using the geohash as the key
5. Saves the result in parquet format partitioned by country and city

## Key Implementation Details

### Data Validation
I filter out any restaurant or weather data that has null values for latitude or longitude since they can't be used for geohashing anyway.

### Geohashing
Decided to use 4-character precision for the geohashes after testing different options:
- 3 chars was too imprecise (matched locations too far apart)
- 5 chars was too precise (many locations that should match didn't)
- 4 chars gives about a 20km² precision which worked best

### Joining Approach
Used a left join to keep all restaurants even when no weather data exists. This maintains data integrity and still provides value for the restaurants with weather data.

### Avoiding Duplicate Columns
Had to explicitly select columns since both datasets have "city" and "country" columns.

### Partitioning Strategy
Partitioning by country and city made sense for the expected access patterns - most queries will filter on these dimensions.

## Project Structure

- `src/main/java/com/epam/itpu/spark/`
  - `SparkApp.java` - Main application entry point
  - `service/DataProcessor.java` - Data loading and processing logic
  - `util/GeohashUtil.java` - Geohash UDF implementation
  - `model/` - Data model classes (not used much - primarily working with DataFrames)
- `data_input/` - Input data directory
- `data_output/` - Output directory for processed data

## Running the Project

To build:
```
mvn clean package
```

To run:
```
java -cp target/Spark-1.0-SNAPSHOT-jar-with-dependencies.jar com.epam.itpu.spark.SparkApp
```

Or with Maven:
```
mvn exec:java -Dexec.mainClass="com.epam.itpu.spark.SparkApp"
```

## Lessons Learned

- Initially had issues with null data in lat/lng columns causing errors - added proper filtering
- Geohash precision is a tradeoff between false positives and false negatives
- Spark's dynamic partition overwrite mode is essential for idempotent operations

## Input Data Format

The restaurant data contains franchise information, location data, and identifiers:
- id, franchise_id, franchise_name, restaurant_franchise_id, country, city, lat, lng

The weather data contains temperature readings:
- lng, lat, avg_tmpr_c, wthr_date, city, country

## Output

The enriched data contains restaurant information enhanced with temperature data, partitioned by country and city for efficient querying.
