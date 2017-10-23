/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 *
 * @author Ethan
 */
public class MovieRatingsMapper 
            extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    IntWritable one = new IntWritable(1);
    Text rating = new Text();
    Text movieId = new Text();
    Text userId = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
      
        // gets a line of text 
        String row = value.toString();
        
        System.out.println("in map row value is: " + row);
        
        // split the tab delimited entry
        String[] cols = row.split("\t");

        try {
            System.out.println("in map split values userid: " + cols[0]);
        } 
        catch (ArrayIndexOutOfBoundsException ae) {
            System.out.println("cols[0] in row " + row + " threw ae.getMessage()");
        }
        
        try {
            System.out.println("in map split values movieid: " + cols[1]);
        }
        catch (ArrayIndexOutOfBoundsException ae) {
            System.out.println("cols[1] in row " + row + " threw ae.getMessage()");
        }
        
        try {
            System.out.println("in map split values rating: " + cols[2]);
        }
        catch (ArrayIndexOutOfBoundsException ae) {
            System.out.println("cols[2] in row " + row + " threw ae.getMessage()");
        } 
        
        // returns a rating value
        // String ratingValue = ratingExtractor(row);
        
        // set the Text userid for a subsequent write op
        userId.set(cols[0]);
        // sets the Text movieid for a subsequent write op
        movieId.set(cols[1]);
        // sets the Text rating for a subsequent write op
        rating.set(cols[2]);

        context.write(userId, one);
        context.write(movieId, one);
        context.write(rating, one);

    }

    // adapted from: 
    // stackoverflow.com/questions/8227003/java-pattern-with-tab-characters
    private static String ratingExtractor(String row) {
        String ratingValue = "";
        String regex = "(?:[^\\t]*)\\t(?:[^\\t]*)\\t([^\\t]*)\\t(?:[^\\t]*)";
        Pattern pattern = Pattern.compile(regex);

        Matcher matcher = pattern.matcher(row);
        if (matcher.matches()) {
          ratingValue = matcher.group(1);
        } 

        return ratingValue;
    }
}
