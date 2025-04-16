package com.epam.itpu.streaming.util;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import ch.hsr.geohash.GeoHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;

/**
 * Utility class for geohashing operations.
 * Geohash is a public domain geocoding system that encodes a geographic location into
 * a short string of letters and digits.
 */
public class GeohashUtil implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(GeohashUtil.class);
    private static final String BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz";
    
    /**
     * Register a UDF to convert lat/lng to geohash
     * Using precision 4 for reasonable area matching without too many false positives
     */
    public static void registerGeohashUDF(SparkSession spark) {
        logger.info("Registering geohash UDF");
        
        // Create UDF that takes lat and lng and returns geohash string
        UDF2<Double, Double, String> geohashUDF = (Double lat, Double lng) -> {
            // Null check to prevent runtime errors
            if (lat == null || lng == null) {
                return null;
            }
            
            // Tried precision 5 but it was too specific, 4 works better for matching
            return GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 4);
        };
        
        // Register with Spark SQL
        spark.udf().register("geohashUDF", geohashUDF, DataTypes.StringType);
        logger.info("Geohash UDF registered successfully");
    }
    
    /**
     * Encodes latitude/longitude to geohash with the specified precision.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @param precision Precision level (number of characters in the geohash)
     * @return The generated geohash
     */
    public static String encodeGeohash(double latitude, double longitude, int precision) {
        double[] latitudeRange = {-90.0, 90.0};
        double[] longitudeRange = {-180.0, 180.0};
        
        StringBuilder geohash = new StringBuilder();
        boolean isEven = true;
        int bit = 0;
        int ch = 0;
        
        while (geohash.length() < precision) {
            if (isEven) {
                double mid = (longitudeRange[0] + longitudeRange[1]) / 2;
                if (longitude >= mid) {
                    ch |= (1 << (4 - bit));
                    longitudeRange[0] = mid;
                } else {
                    longitudeRange[1] = mid;
                }
            } else {
                double mid = (latitudeRange[0] + latitudeRange[1]) / 2;
                if (latitude >= mid) {
                    ch |= (1 << (4 - bit));
                    latitudeRange[0] = mid;
                } else {
                    latitudeRange[1] = mid;
                }
            }
            
            isEven = !isEven;
            
            if (bit < 4) {
                bit++;
            } else {
                geohash.append(BASE32.charAt(ch));
                bit = 0;
                ch = 0;
            }
        }
        
        return geohash.toString();
    }
    
    /**
     * Rounds a double value to specified decimal places
     *
     * @param value The value to round
     * @param places Number of decimal places
     * @return Rounded value
     */
    public static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();
        
        long factor = (long) Math.pow(10, places);
        value = value * factor;
        long tmp = Math.round(value);
        return (double) tmp / factor;
    }
}
