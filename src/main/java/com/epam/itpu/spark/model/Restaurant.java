package com.epam.itpu.spark.model;

import java.io.Serializable;

public class Restaurant implements Serializable {
    private String id;
    private String franchiseId;
    private String franchiseName;
    private String restaurantFranchiseId;
    private String country;
    private String city;
    private Double lat;
    private Double lng;
    private String geohash;

    // Getters and setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getFranchiseId() { return franchiseId; }
    public void setFranchiseId(String franchiseId) { this.franchiseId = franchiseId; }
    
    public String getFranchiseName() { return franchiseName; }
    public void setFranchiseName(String franchiseName) { this.franchiseName = franchiseName; }
    
    public String getRestaurantFranchiseId() { return restaurantFranchiseId; }
    public void setRestaurantFranchiseId(String restaurantFranchiseId) { this.restaurantFranchiseId = restaurantFranchiseId; }
    
    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }
    
    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }
    
    public Double getLat() { return lat; }
    public void setLat(Double lat) { this.lat = lat; }
    
    public Double getLng() { return lng; }
    public void setLng(Double lng) { this.lng = lng; }
    
    public String getGeohash() { return geohash; }
    public void setGeohash(String geohash) { this.geohash = geohash; }
}
