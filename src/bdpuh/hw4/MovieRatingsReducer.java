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
 * @author Ethan
 */
public class MovieRatingsReducer 
            extends Reducer<IntWritable, Text, IntWritable, Text> {
    
    int i = 0;
    // IntWritable count = new IntWritable();
    Text mapVal = new Text();
    
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
    
            i = 0;
            for (Text val : values) {
                mapVal.set(val);
                // i = i + val
                System.out.println("Reducer for loop: "  + val);
            }
            
            // count.set(i);
            context.write(key, mapVal);
            
//            System.out.println(
//                    "Reducer after context.write. key: " + key + "count: " + count);
            
            
    }
}
