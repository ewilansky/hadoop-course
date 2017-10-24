/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw4;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ethanw
 */
public class MovieRatingsCombiner 
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    int i = 0;
    int sumUserIds = 0;
    int sumRatings = 0;
    int totalRating = 0;
    IntWritable count = new IntWritable();
    
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values,
        Context context) 
            throws IOException, InterruptedException {
        
        i = 0;
        sumUserIds = 0;
        sumRatings = 0;
        totalRating = 0;
        
        IntWritable userIdsCount = new IntWritable();
        IntWritable ratingsCount = new IntWritable();
        IntWritable ratingsTotal = new IntWritable();
        
        System.out.println("In combiner");
        
        for (IntWritable val : values) {
            // values repeat from mapper: userId, movieId, rating ...
            System.out.println("i mod3 is :" + i % 3);
            switch (i % 3) {
                case 0: // userid count
                    sumUserIds = sumUserIds + 1;
                    break;
                case 1: // ratings count
                    sumRatings = sumRatings + 1;
                    break;
                case 2: // ratings sum
                    totalRating = totalRating + val.get();
                    break;
                default:
                    break;
            }
            
            
            i = i + 1;
        } 
        
        userIdsCount.set(sumUserIds);
        ratingsCount.set(sumRatings);
        context.write(key, userIdsCount);
        context.write(key, ratingsCount);
        context.write(key, ratingsTotal);
    }
}
