/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw4;

/**
 *
 * @author ethanw
 */
public class MovieMetric {
    private Integer _idKey;
    private String _title; 
    private String _releaseDate;
    private String _imDbUrl;
    private Integer _uniqueUsers;
    private Integer _ratings;
    private Integer _ratingsAverage;
    
    public MovieMetric(){};
    
    public MovieMetric(
            int idKey, String title, String releaseDate, String iMDbUrL, 
            int uniqueUsers, int ratings, int ratingsAverage) 
    {
        this._idKey = idKey;
        this._title = title;
        this._releaseDate = releaseDate;
        this._imDbUrl = iMDbUrL;
        this._uniqueUsers = uniqueUsers;
        this._ratings = ratings;
        this._ratingsAverage = ratingsAverage;    
    }
    
    public int getId() { 
        return this._idKey; 
    }
    
    public void setId(int idKey) { 
        this._idKey = idKey; 
    }

    public String getTitle() {
        return _title;
    }

    public void setTitle(String _title) {
        this._title = _title;
    }

    public String getReleaseDate() {
        return _releaseDate;
    }

    public void setReleaseDate(String _releaseDate) {
        this._releaseDate = _releaseDate;
    }

    public String getImDbUrl() {
        return _imDbUrl;
    }

    public void setImDbUrl(String _imDbUrl) {
        this._imDbUrl = _imDbUrl;
    }

    public int getUniqueUsers() {
        return _uniqueUsers;
    }

    public void setUniqueUsers(int _uniqueUsers) {
        this._uniqueUsers = _uniqueUsers;
    }

    public int getRatings() {
        return _ratings;
    }

    public void setRatings(int _ratings) {
        this._ratings = _ratings;
    }

    public int getRatingsAverage() {
        return _ratingsAverage;
    }

    public void setRatingsAverage(int _ratingsSum) {
        this._ratingsAverage = _ratingsSum;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        sb
            .append(this._idKey.toString())
            .append("\t")
            .append(this._title)
            .append("\t")
            .append(this._releaseDate)
            .append("\t")
            .append(this._imDbUrl)
            .append("\t")
            .append(this._uniqueUsers.toString())
            .append("\t")
            .append(this._ratings.toString())
            .append("\t")
            .append(this._ratingsAverage.toString());
        
        return sb.toString();
    }
}
