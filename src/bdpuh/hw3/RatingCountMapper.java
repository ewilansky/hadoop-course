/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw3;

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
public class RatingCountMapper 
            extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    IntWritable one = new IntWritable(1);
    Text rating = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
      
        // gets a line of text 
        String row = value.toString();
        
        // returns a rating value
        String ratingValue = ratingExtractor(row);
        
        // sets the Text rating for a subsequent write op
        rating.set(ratingValue);
       
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
