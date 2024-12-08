package com.restauran.geodata.geodata_enrichment;

import java.io.*;
import org.apache.commons.csv.*;

public class DataCheckerWeather {
		    public static void main(String[] args) {
	        // Define the array of input files here
	        String[] inputFiles = {
	            "C:\\Users\\admin\\Desktop\\TechOrdaEPAM_Albina\\spark\\restaurant_csv\\restaurant_csv\\part-00000-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv",
	            "C:\\Users\\admin\\Desktop\\TechOrdaEPAM_Albina\\spark\\restaurant_csv\\restaurant_csv\\part-00001-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv",
	            "C:\\Users\\admin\\Desktop\\TechOrdaEPAM_Albina\\spark\\restaurant_csv\\restaurant_csv\\part-00002-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv",
	            "C:\\Users\\admin\\Desktop\\TechOrdaEPAM_Albina\\spark\\restaurant_csv\\restaurant_csv\\part-00003-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv",
	            "C:\\Users\\admin\\Desktop\\TechOrdaEPAM_Albina\\spark\\restaurant_csv\\restaurant_csv\\part-00004-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv"
	        };
	        
	        String OUTPUT_FILE_PATH = "output_with_geohash_combined.csv"; // Specify the output file path

	        try {
	            // Open the output file for writing
	            BufferedWriter writer = new BufferedWriter(new FileWriter(OUTPUT_FILE_PATH));
	            writer.write("id,franchise_id,franchise_name,restaurant_franchise_id,country,city,lat,lng,geohash\n");

	            // Process each CSV file in the array
	            for (String inputFile : inputFiles) {
	                System.out.println("Starting to process file: " + inputFile);
	                processCsvFile(inputFile, writer);
	            }

	            // Close the writer after processing all files
	            writer.close();
	            System.out.println("Processing completed. Results saved in: " + OUTPUT_FILE_PATH);
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }

	    public static void processCsvFile(String filePath, BufferedWriter writer) throws IOException {
	        System.out.println("Processing file: " + filePath);
	        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
	            CSVParser csvParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
	            for (CSVRecord record : csvParser) {
	                String id = record.get("id");
	                String franchiseId = record.get("franchise_id");
	                String franchiseName = record.get("franchise_name");
	                String restaurantFranchiseId = record.get("restaurant_franchise_id");
	                String country = record.get("country");
	                String city = record.get("city");
	                String lat = record.get("lat");
	                String lng = record.get("lng");

	                System.out.println("Processing record: " + id);

	                // Check for invalid coordinates (0.0, 0.0)
	                if (!lat.equals("0.0") && !lng.equals("0.0")) {
	                    String geohash = generateGeohash(Double.parseDouble(lat), Double.parseDouble(lng));
	                    System.out.println("Generated geohash for " + id + ": " + geohash);
	                    
	                    // Write the record to the output file
	                    writer.write(id + "," + franchiseId + "," + franchiseName + "," + restaurantFranchiseId + ","
	                            + country + "," + city + "," + lat + "," + lng + "," + geohash + "\n");
	                } else {
	                    System.out.println("Skipping record " + id + " due to invalid coordinates (" + lat + "," + lng + ")");
	                }
	            }
	        }
	    }

	    // Correct Geohash generation method
	    public static String generateGeohash(double lat, double lon) {
	        StringBuilder geohash = new StringBuilder();
	        
	        double latMin = -90.0;
	        double latMax = 90.0;
	        double lonMin = -180.0;
	        double lonMax = 180.0;
	        
	        int precision = 4;  // Generate a 4-character geohash
	        
	        for (int i = 0; i < precision; i++) {
	            double midLat = (latMin + latMax) / 2;
	            double midLon = (lonMin + lonMax) / 2;
	            
	            if (lat > midLat) {
	                geohash.append("1");
	                latMin = midLat;
	            } else {
	                geohash.append("0");
	                latMax = midLat;
	            }

	            if (lon > midLon) {
	                geohash.append("1");
	                lonMin = midLon;
	            } else {
	                geohash.append("0");
	                lonMax = midLon;
	            }
	        }

	        return geohash.toString();
	    }
	}
