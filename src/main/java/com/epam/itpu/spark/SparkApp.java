package com.epam.itpu.spark;

import com.epam.itpu.spark.service.DataProcessor;
import com.epam.itpu.spark.util.GeohashUtil;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;

/**
 * Main application class for restaurant and weather data processing
 */
public class SparkApp {
    private static final Logger logger = LoggerFactory.getLogger(SparkApp.class);
    private static final String RESTAURANT_DATA_PATH = "data_input/receipt_restaurants";
    private static final String WEATHER_DATA_PATH = "data_input/weather";
    private static final String BATCH_OUTPUT_PATH = "data_output/batch";
    private static final String STREAMING_INPUT_PATH = "streaming_input";
    private static final String STREAMING_OUTPUT_PATH = "data_output/streaming";

    public static void main(String[] args) throws TimeoutException {
        SparkSession spark = null;
        try {
            logger.info("Starting Spark application");

            // Create Spark session - added dynamic partitioning for better overwrites
            spark = SparkSession.builder()
                    .appName("RestaurantWeatherJoin")
                    .master("local[*]")
                    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                    .getOrCreate();

            // Turn down the noise
            spark.sparkContext().setLogLevel("ERROR");

            // Register geohash UDF for coordinate encoding
            logger.info("Registering geohash UDF");
            GeohashUtil.registerGeohashUDF(spark);

            DataProcessor processor = new DataProcessor(spark);
            
            // Still call registerUDFs to register named UDFs in the catalog
            processor.registerUDFs();

            // Determine execution mode based on arguments
            if (args.length > 0 && args[0].equalsIgnoreCase("streaming")) {
                // Execute streaming job
                runStreamingJob(spark, processor);
            } else {
                // Execute batch processing job
                runBatchJob(processor);
            }

            logger.info("Processing completed successfully");
        } catch (Exception e) {
            logger.error("Application failed: ", e);
            System.exit(1);
        } finally {
            if (spark != null) {
                logger.info("Stopping Spark session");
                spark.stop();
            }
        }
    }

    /**
     * Run the batch processing job
     *
     * @param processor DataProcessor instance
     */
    private static void runBatchJob(DataProcessor processor) {
        System.out.println("Starting batch processing job...");
        
        // Process restaurant and weather data
        System.out.println("Processing restaurant data...");
        Dataset<Row> restaurantDf = processor.processRestaurantData(RESTAURANT_DATA_PATH);
        
        System.out.println("Processing weather data...");
        Dataset<Row> weatherDf = processor.processWeatherData(WEATHER_DATA_PATH);
        
        // Enrich restaurant data with weather data
        System.out.println("Joining restaurant and weather data...");
        Dataset<Row> enrichedDf = processor.enrichRestaurantWithWeather(restaurantDf, weatherDf);
        
        // Calculate receipt metrics and order size categories
        System.out.println("Calculating receipt metrics...");
        Dataset<Row> resultDf = processor.calculateReceiptMetrics(enrichedDf);
        
        // Rename the restaurant_date column to date before saving
        resultDf = resultDf.withColumnRenamed("restaurant_date", "date");
        
        // Save results to CSV
        System.out.println("Saving results to CSV...");
        processor.saveToCSV(resultDf, BATCH_OUTPUT_PATH);
        
        System.out.println("Batch processing completed. Results saved to " + BATCH_OUTPUT_PATH);
    }

    /**
     * Run the streaming job
     *
     * @param spark SparkSession instance
     * @param processor DataProcessor instance
     */
    private static void runStreamingJob(SparkSession spark, DataProcessor processor) throws TimeoutException {
        System.out.println("Starting streaming job...");

        try {
            // Read streaming data from input directory
            Dataset<Row> streamingDf = spark.readStream()
                    .format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .schema(getStreamingSchema(spark))
                    .load(STREAMING_INPUT_PATH);

            // Process streaming data
            Dataset<Row> processedDf = processor.processStreamingData(streamingDf);

            // Start the streaming query
            StreamingQuery query = processor.writeStreamingToCSV(processedDf, STREAMING_OUTPUT_PATH);

            // Await termination
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            System.err.println("Streaming query exception: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Define schema for streaming input data
     *
     * @param spark SparkSession instance
     * @return StructType schema
     */
    private static StructType getStreamingSchema(SparkSession spark) {
        // Create schema matching the batch output
        return new StructType()
                .add("id", DataTypes.StringType)
                .add("franchise_id", DataTypes.StringType)
                .add("franchise_name", DataTypes.StringType)
                .add("restaurant_franchise_id", DataTypes.StringType)
                .add("country", DataTypes.StringType)
                .add("city", DataTypes.StringType)
                .add("lat", DataTypes.DoubleType)
                .add("lng", DataTypes.DoubleType)
                .add("date", DataTypes.DateType)
                .add("avg_tmpr_c", DataTypes.DoubleType)
                .add("erroneous_orders_data", DataTypes.LongType)
                .add("tiny_orders", DataTypes.LongType)
                .add("small_orders", DataTypes.LongType)
                .add("medium_orders", DataTypes.LongType)
                .add("large_orders", DataTypes.LongType)
                .add("most_popular_order_type", DataTypes.StringType)
                .add("receipt_id", DataTypes.StringType)
                .add("total_cost", DataTypes.DoubleType)
                .add("discount", DataTypes.DoubleType)
                .add("real_total_cost", DataTypes.DoubleType)
                .add("items_count", DataTypes.IntegerType)
                .add("order_size", DataTypes.StringType);
    }
}