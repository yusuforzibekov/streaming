package com.epam.itpu.spark.model;

import java.io.Serializable;
import java.sql.Date;

public class Weather implements Serializable {
    private Double lng;
    private Double lat;
    private Double avgTmprC;
    private Date wthrDate;
    private String city;
    private String country;
    
    // Getters and setters
    public Double getLng() { return lng; }
    public void setLng(Double lng) { this.lng = lng; }
    
    public Double getLat() { return lat; }
    public void setLat(Double lat) { this.lat = lat; }
    
    public Double getAvgTmprC() { return avgTmprC; }
    public void setAvgTmprC(Double avgTmprC) { this.avgTmprC = avgTmprC; }
    
    public Date getWthrDate() { return wthrDate; }
    public void setWthrDate(Date wthrDate) { this.wthrDate = wthrDate; }
    
    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }
    
    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }
}
