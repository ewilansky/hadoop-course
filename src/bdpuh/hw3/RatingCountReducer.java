/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Ethan
 */
public class RatingCountReducer 
            extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    int i = 0;
    IntWritable count = new IntWritable();
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
    
            i = 0;
            for (IntWritable val :  values) {
                i = i + 1;
            }
            
            count.set(i);
            context.write (key, count);
    }
}
