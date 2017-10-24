/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw4;

import java.io.IOException;
import java.util.StringJoiner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author Ethan
 */
public class MovieRatingsMapper 
            extends Mapper<LongWritable, Text, IntWritable, Text> {
           
    IntWritable movieIdKey = new IntWritable();

    // use this to set one for # of users and # of ratings
    IntWritable one = new IntWritable(1);
    // use this to set the value of a rating
    IntWritable ratingsValue = new IntWritable();
    
    StringJoiner joiner = new StringJoiner(",");
    
    Text dataRow = new Text();
    
    // StringJoiner joiner = new StringJoiner(",");
    // joiner.add("01").add("02").add("03");
    // String joinedString = joiner.toString(); // "01,02,03"

    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
      
        // gets a line of text 
        String row = value.toString();
        
        System.out.println("in map row value is: " + row);
        
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        
        if (fileName.endsWith(".data")) {
            // split the tab delimited entry
            String[] cols = row.split("\t");

            movieIdKey.set(Integer.parseInt(cols[1]));

            joiner.add(cols[0]).add(cols[2]);
        } else {
        
        }
       
        
//        context.write(movieIdKey, one); // one for userId
//        context.write(movieIdKey, one); // one for rating
//        context.write(movieIdKey, ratingsValue); // actual rating value

    }
}
