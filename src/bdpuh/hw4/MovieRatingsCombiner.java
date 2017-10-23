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
    int movieId_userId = 0;
    int movieId_rating = 0;
    // int rating = 0;
    IntWritable userIdCount = new IntWritable();
    IntWritable ratingCount = new IntWritable();
    // IntWritable movieIdCount = new IntWritable();
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
        Context context) 
            throws IOException, InterruptedException {
        
        i = 0;
        movieId_userId = 0;
        movieId_rating = 0;
        // rating = 0;
        
        System.out.println("In combiner");
        
        for (IntWritable val : values) {
            // values repeat from mapper: userId, movieId, rating ...
            System.out.println("i mod2 is :" + i % 2);
            
            switch (i % 2) {
                case 0: // Movie Id - UserId 
                    key.set(key + ":MovieId-UserId");
                    movieId_userId = movieId_userId + 1;
                    System.out.println(
                            "userid combiner i: " + i + "val:" + val.get());
                    break;
                case 1: // Movie Id - Rating 
                    key.set(key + ":MovieId-Rating");
                    movieId_rating = movieId_rating + 1;
                    System.out.println(
                            "rating combiner i: " + i + "val:" + val.get());
                    break;
//                case 2: // Movie Id 
//                    key.set(key + "movie-id-");
//                    movieId = movieId + 1;
//                    System.out.println(
//                            "movie id combiner i: " + i + "val:" + val.get());                    
//                    break;
                default:
                    break;
            }
  
            i = i + 1;
        } 
        
        userIdCount.set(movieId_userId);
        context.write (key, userIdCount);
        
        // movieIdCount.set(movieId);
        // context.write (key, movieIdCount);
        
        ratingCount.set(movieId_rating);
        context.write (key, ratingCount);
   
    }
}
