/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw4;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ethanw
 */
public class MovieRatingsCombinerOri 
            extends Reducer<CompositeKeyWritable, Text, CompositeKeyWritable, Text> {

    int i = 0;
    IntWritable usersCount = new IntWritable();
    IntWritable ratingsCount = new IntWritable();
    
    @Override
    protected void reduce(CompositeKeyWritable key, Iterable<Text> values,
        Context context) 
            throws IOException, InterruptedException {
        
        Text thisValue = null;
        
        i = 0;
        for (Text val : values) {
            // for each value with a matching key, 
            // usersCount total number of users
            // usersCount total number of ratings
            i = i + 1;
            
            thisValue = val;
        }
            
        usersCount.set(i);
        
        context.write (key, thisValue);
    }
}
