/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw4;

import java.io.IOException;
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
            extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
           
    IntWritable movieIdKey = new IntWritable();

    // use this to set one for # of users and # of ratings
    IntWritable one = new IntWritable(1);
    // use this to set the value of a rating
    IntWritable ratingsValue = new IntWritable();

    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
      
        // gets a line of text 
        String row = value.toString();
        
        System.out.println("in map row value is: " + row);
        
        // split the tab delimited entry
        String[] cols = row.split("\t");
        
        movieIdKey.set(Integer.parseInt(cols[1]));
       
        // userId.set(cols[0]);
        ratingsValue.set(Integer.parseInt(cols[2]));
        
        context.write(movieIdKey, one); // one for userId
        context.write(movieIdKey, one); // one for rating
        context.write(movieIdKey, ratingsValue); // actual rating value

    }
}
