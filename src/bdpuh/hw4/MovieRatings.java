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

/**
 *
 * @author ethanw
 */
public class MovieRatings 
            extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    // the map output key will be the movie id (ux.data) <=> movie id (u.item) - called item id
    
    // add attribute srcIndex to tag the identity of the data
        // data (ux.data) or movie (u.item)
    
    // map output key - cmoposite of srcIndex + movie id
    
    // partition on key created with movie id
    
    // sort on movie id and then srcIndex
    
    // group the data based on movie id key
    
    // iterate over items and join on movie id
    
    
    
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        // load the u.item file into memory (this will happen at every mapper)
        
        // get a line of text from a ux.data file
        String row = value.toString();
    }
}
