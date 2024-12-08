package com.restauran.geodata.geodata_enrichment;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions.*;

import java.util.Arrays;

public class DataJoiner {

    public static void main(String[] args) {
        // Set the Hadoop home directory (if running on Windows)
        System.setProperty("hadoop.home.dir", "C:\\Users\\admin\\hadoop\\bin");  // Adjust path accordingly

        // Create a Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Join Weather and Restaurant Data")
                .config("spark.master", "local")  // Use local mode for development
                .config("spark.hadoop.security.authentication", "simple")  // Disable Kerberos security
                .getOrCreate();

     // Array of restaurant CSV file paths
        String[] restaurantCsvPaths = {
                "C:\\Users\\admin\\Desktop\\TechOrdaEPAM_Albina\\spark\\restaurant_csv\\restaurant_csv\\part-00000-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv",
                "C:\\Users\\admin\\Desktop\\TechOrdaEPAM_Albina\\spark\\restaurant_csv\\restaurant_csv\\part-00001-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv",
                "C:\\Users\\admin\\Desktop\\TechOrdaEPAM_Albina\\spark\\restaurant_csv\\restaurant_csv\\part-00002-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv",
                "C:\\Users\\admin\\Desktop\\TechOrdaEPAM_Albina\\spark\\restaurant_csv\\restaurant_csv\\part-00003-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv",
                "C:\\Users\\admin\\Desktop\\TechOrdaEPAM_Albina\\spark\\restaurant_csv\\restaurant_csv\\part-00004-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv"
        
        };
     // Path to the directory containing the weather Parquet files
        String weatherParquetPath = "C:\\Users\\admin\\Desktop\\TechOrdaEPAM_Albina\\spark\\restaurant_csv\\weather\\*.parquet";  // Use wildcard to read all files


        // Load the restaurant data from CSV files
        Dataset<Row> restaurantData = spark.read()
                .option("header", "true")  // assuming the first row has column names
                .csv(restaurantCsvPaths);

        // Load the weather data from Parquet files
        Dataset<Row> weatherData = spark.read()
                .parquet(weatherParquetPath);

        // Perform the left join on the geohash (4-character geohash)
        Dataset<Row> enrichedData = restaurantData.join(
                weatherData,
                restaurantData.col("geohash").equalTo(weatherData.col("geohash")),
                "left"
        );

        // Store the enriched data to Parquet format with partitioning by geohash
        enrichedData.write()
                .mode("overwrite")  // Overwrite the existing data if any
                .partitionBy("geohash")  // Partition by geohash
                .parquet("C:\\Users\\admin\\Desktop\\TechOrdaEPAM_Albina\\spark\\restaurant_csv\\enriched_data");

        // Show the result in the console for validation
        enrichedData.show();
        
        // Stop the Spark session
        spark.stop();
    }
}

