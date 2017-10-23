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
            extends Mapper<LongWritable, Text, IntWritable, Text> {
    
    // IntWritable one = new IntWritable(1);
    
    IntWritable movieIdKey = new IntWritable();
    
    Text userId = new Text();
    Text rating = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
      
        // gets a line of text 
        String row = value.toString();
        
        System.out.println("in map row value is: " + row);
        
        // split the tab delimited entry
        String[] cols = row.split("\t");
        
        movieIdKey.set(Integer.parseInt(cols[1]));
       
        userId.set(cols[0].toString());
        rating.set(cols[2].toString());
        
        context.write(movieIdKey, userId);
        context.write(movieIdKey, rating);

    }
}
