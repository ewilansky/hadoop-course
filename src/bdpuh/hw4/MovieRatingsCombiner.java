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
public class MovieRatingsCombiner 
            extends Reducer<Text, IntWritable, IntWritable, IntWritable > {

    int i = 0;
    IntWritable count = new IntWritable();
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
        Context context) 
            throws IOException, InterruptedException {
        
        i = 0;
        // movieId_userId = 0;
        // movieId_rating = 0;
        // rating = 0;
        
        System.out.println("In combiner");
        
        for (IntWritable val : values) {
            // values repeat from mapper: userId, movieId, rating ...
            // System.out.println("i mod2 is :" + i % 2);
            i = i + val.get();

        } 
        
        // count.set(i);
        // context.write(key, count);
   
    }
}
