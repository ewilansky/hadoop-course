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
            extends Reducer<Text, IntWritable, Text, IntWritable> {

    int i = 0;
    int userId = 0;
    int movieId = 0;
    int rating = 0;
    IntWritable userIdCount = new IntWritable();
    IntWritable ratingCount = new IntWritable();
    IntWritable movieIdCount = new IntWritable();
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
        Context context) 
            throws IOException, InterruptedException {
        
        i = 0;
        userId = 0;
        movieId = 0;
        rating = 0;
        
        for (IntWritable val : values) {
            // values repeat from mapper: userId, movieId, rating ...
            int mod3 = i % 3;
            switch (i % 3) {
                case 1: // User Id 
                    userId = userId + val.get();
                    System.out.println(
                            "user id combiner i: " + i + "val:" + val.get());
                    break;
                case 2: // Movie Id 
                    movieId = movieId + val.get();
                    System.out.println(
                            "movie id combiner i: " + i + "val:" + val.get());                    
                    break;
                case 3: // Rating 
                    rating = rating + val.get();
                    System.out.println(
                            "rating combiner i: " + i + "val:" + val.get());
                    break;
                default:
                    break;
            }
  
            i += i;
        } 
        
        userIdCount.set(userId);
        context.write (key, userIdCount);
        
        movieIdCount.set(movieId);
        context.write (key, movieIdCount);
        
        ratingCount.set(rating);
        context.write (key, ratingCount);
   
    }
}
