package com.restauran.geodata.geodata_enrichment;

public class GeoHashGenerator {
	// Geohash generation using a custom method (simplified version):
	public static String generateGeohash(double lat, double lon) {
	    StringBuilder geohash = new StringBuilder();
	    
	    // Define the bounds for the latitude and longitude.
	    double latMin = -90.0;
	    double latMax = 90.0;
	    double lonMin = -180.0;
	    double lonMax = 180.0;

	    // Geohash precision: how many bits (4 characters) you want for your geohash
	    int precision = 4;  // Here we generate a 4-character geohash.

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
	}}
