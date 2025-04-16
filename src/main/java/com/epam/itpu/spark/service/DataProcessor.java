package com.epam.itpu.spark.service;

import com.epam.itpu.spark.util.GeohashUtil;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.streaming.StreamingQuery;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class DataProcessor {

    private final SparkSession spark;
    // Define UDFs as class members to reuse them
    private final UserDefinedFunction roundCoordinateUDF;
    private final UserDefinedFunction calculateOrderSizeUDF;

    public DataProcessor(SparkSession spark) {
        this.spark = spark;
        
        // Initialize UDFs during construction
        this.roundCoordinateUDF = udf(
            (Double value) -> value != null ? GeohashUtil.round(value, 2) : null, 
            DataTypes.DoubleType);
            
        this.calculateOrderSizeUDF = udf(
            (Integer itemsCount) -> {
                if (itemsCount == null || itemsCount <= 0) {
                    return "Erroneous";
                } else if (itemsCount == 1) {
                    return "Tiny";
                } else if (itemsCount >= 2 && itemsCount <= 3) {
                    return "Small";
                } else if (itemsCount >= 4 && itemsCount <= 10) {
                    return "Medium";
                } else {
                    return "Large";
                }
            }, 
            DataTypes.StringType);
    }

    /**
     * Process restaurant data: convert date formats and round coordinates
     * 
     * @param restaurantDataPath Path to restaurant CSV file
     * @return Processed restaurant DataFrame
     */
    public Dataset<Row> processRestaurantData(String restaurantDataPath) {
        // Read restaurant data
        Dataset<Row> restaurantDf = spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(restaurantDataPath);
        
        // Debug - show available columns
        System.out.println("Restaurant DataFrame schema:");
        restaurantDf.printSchema();
        
        // Convert date_time to date format and round coordinates
        Dataset<Row> processedDf = restaurantDf
            .filter(year(to_date(col("date_time"), "yyyy-MM-dd")).equalTo(2022))
            .withColumn("restaurant_date", to_date(col("date_time"), "yyyy-MM-dd"))
            .withColumn("lat", roundCoordinateUDF.apply(col("lat")))
            .withColumn("lng", roundCoordinateUDF.apply(col("lng")));
        
        // Debug - show the processed data
        System.out.println("Processed restaurant data sample:");
        processedDf.show(5);
        
        return processedDf;
    }

    /**
     * Process weather data: convert date formats and round coordinates
     * 
     * @param weatherDataPath Path to weather CSV file
     * @return Processed weather DataFrame
     */
    public Dataset<Row> processWeatherData(String weatherDataPath) {
        // Read weather data
        Dataset<Row> weatherDf = spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(weatherDataPath);
        
        // Debug - show available columns
        System.out.println("Weather DataFrame schema:");
        weatherDf.printSchema();
        
        // Convert wthr_date to date format and round coordinates
        Dataset<Row> processedDf = weatherDf
            .filter(year(to_date(col("wthr_date"), "yyyy-MM-dd")).equalTo(2022))
            .withColumn("weather_date", to_date(col("wthr_date"), "yyyy-MM-dd"))
            .withColumn("lat", roundCoordinateUDF.apply(col("lat")))
            .withColumn("lng", roundCoordinateUDF.apply(col("lng")));
        
        // Debug - show the processed data
        System.out.println("Processed weather data sample:");
        processedDf.show(5);
        
        return processedDf;
    }

    /**
     * Register UDFs for data processing
     */
    public void registerUDFs() {
        // Register UDFs with Spark's function registry
        spark.udf().register("roundCoordinate", 
            (Double value) -> value != null ? GeohashUtil.round(value, 2) : null, 
            DataTypes.DoubleType);
            
        spark.udf().register("calculateOrderSize", 
            (Integer itemsCount) -> {
                if (itemsCount == null || itemsCount <= 0) {
                    return "Erroneous";
                } else if (itemsCount == 1) {
                    return "Tiny";
                } else if (itemsCount >= 2 && itemsCount <= 3) {
                    return "Small";
                } else if (itemsCount >= 4 && itemsCount <= 10) {
                    return "Medium";
                } else {
                    return "Large";
                }
            }, 
            DataTypes.StringType);
    }

    /**
     * Process and enrich restaurant data with weather data
     * 
     * @param restaurantDf Processed restaurant DataFrame
     * @param weatherDf Processed weather DataFrame
     * @return Enriched DataFrame
     */
    public Dataset<Row> enrichRestaurantWithWeather(Dataset<Row> restaurantDf, Dataset<Row> weatherDf) {
        // Debug - show column names before join
        System.out.println("Restaurant columns before join:");
        restaurantDf.columns().toString();
        
        System.out.println("Weather columns before join:");
        weatherDf.columns().toString();
        
        // Rename weather columns to avoid ambiguity with restaurant columns
        Dataset<Row> renamedWeatherDf = weatherDf
            .withColumnRenamed("country", "weather_country")
            .withColumnRenamed("city", "weather_city")
            .withColumnRenamed("lat", "weather_lat")
            .withColumnRenamed("lng", "weather_lng");
        
        // Join restaurant and weather data on date and coordinates
        Dataset<Row> joinedDf = restaurantDf.join(renamedWeatherDf,
            restaurantDf.col("restaurant_date").equalTo(renamedWeatherDf.col("weather_date"))
                .and(restaurantDf.col("lat").equalTo(renamedWeatherDf.col("weather_lat")))
                .and(restaurantDf.col("lng").equalTo(renamedWeatherDf.col("weather_lng"))),
            "inner");
        
        // Filter by temperature > 0°C
        return joinedDf.filter(col("avg_tmpr_c").gt(0.0));
    }

    /**
     * Calculate receipt metrics and order size categories
     * 
     * @param enrichedDf Enriched DataFrame from the join
     * @return DataFrame with calculated metrics
     */
    public Dataset<Row> calculateReceiptMetrics(Dataset<Row> enrichedDf) {
        Dataset<Row> receiptDfWithItems = enrichedDf
            .withColumn("items_count", ceil(col("total_cost").divide(lit(10.0))).cast(DataTypes.IntegerType))
            .withColumn("real_total_cost", col("total_cost").minus(col("total_cost").multiply(col("discount"))))
            .withColumn("order_size", calculateOrderSizeUDF.apply(col("items_count")));

        Dataset<Row> orderTypesDf = receiptDfWithItems
            .groupBy(
                col("id"),
                col("franchise_id"),
                col("franchise_name"),
                col("restaurant_franchise_id"),
                col("country"),
                col("city"),
                col("lat"),
                col("lng"),
                col("restaurant_date"),
                col("avg_tmpr_c")
            )
            .agg(
                count(when(col("order_size").equalTo("Erroneous"), 1)).alias("erroneous_orders_data"),
                count(when(col("order_size").equalTo("Tiny"), 1)).alias("tiny_orders"),
                count(when(col("order_size").equalTo("Small"), 1)).alias("small_orders"),
                count(when(col("order_size").equalTo("Medium"), 1)).alias("medium_orders"),
                count(when(col("order_size").equalTo("Large"), 1)).alias("large_orders")
            );

        // Find the most popular order type by comparing the counts directly
        return orderTypesDf.withColumn(
                "most_popular_order_type",
                expr(
                    "CASE " +
                    "WHEN tiny_orders >= small_orders AND tiny_orders >= medium_orders AND tiny_orders >= large_orders THEN 'Tiny' " +
                    "WHEN small_orders >= tiny_orders AND small_orders >= medium_orders AND small_orders >= large_orders THEN 'Small' " +
                    "WHEN medium_orders >= tiny_orders AND medium_orders >= small_orders AND medium_orders >= large_orders THEN 'Medium' " +
                    "ELSE 'Large' END"
                )
            );
    }

    /**
     * Process streaming data and add promo field based on temperature
     * 
     * @param df Input DataFrame from streaming
     * @return Processed DataFrame with promo field
     */
    public Dataset<Row> processStreamingData(Dataset<Row> df) {
        return df.withColumn("promo_cold_drinks", 
                             when(col("avg_tmpr_c").gt(25.0), lit(true))
                             .otherwise(lit(false)));
    }

    /**
     * Save data to CSV in the given output path
     * 
     * @param df DataFrame to save
     * @param outputPath Output directory path
     */
    public void saveToCSV(Dataset<Row> df, String outputPath) {
        df.write()
          .option("header", "true")
          .mode(SaveMode.Overwrite)
          .csv(outputPath);
    }

    /**
     * Write streaming DataFrame to CSV
     * 
     * @param df Streaming DataFrame
     * @param outputPath Output directory path
     * @return StreamingQuery object
     */
    public StreamingQuery writeStreamingToCSV(Dataset<Row> df, String outputPath) throws TimeoutException {
        return df.writeStream()
                .outputMode("append")
                .format("csv")
                .option("path", outputPath)
                .option("checkpointLocation", outputPath + "/checkpoint")
                .start();
    }
}
