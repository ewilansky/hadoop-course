/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw4;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Ethan
 */
public class MovieRatingsReducer 
            extends Reducer<IntWritable, Text, IntWritable, Text> {
    
    int i = 0;
    IntWritable count = new IntWritable();
    StringBuilder sb = new StringBuilder();
    Text OutputRow = new Text();
    MovieMetric movieRow = new MovieMetric();
    
    
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
    
            int valIterator = 0;
            int sumRatings = 0;
            
            System.out.println("In Reduce method line 33");
                    
            for (Text val : values) {
               
                String row = val.toString();
                // split the incoming row
                String[] cols = row.split("\\|");
                
                if ("R".equals(cols[0])) {
                    // since there are the same number of user records and
                    // ratings records, one iterator is enough for setting
                    // both values in this case
                    valIterator = valIterator + 1;
                    sumRatings = sumRatings + Integer.parseInt(cols[2]);
                    
                } else if ("I".equals(cols[0])) {
                    movieRow.setTitle(cols[1]);
                    try {
                        movieRow.setReleaseDate(cols[2]);
                    } catch (ArrayIndexOutOfBoundsException ex) {
                        movieRow.setReleaseDate("No Release Date listed");
                    }
                    
                    try {
                        movieRow.setImDbUrl(cols[3]);
                    } catch (ArrayIndexOutOfBoundsException ex) {
                        movieRow.setImDbUrl("No IMDbUrl listed");
                    }
                    
                    // count unique movies
                    Counter counter = context.getCounter(
                            MovieRatingCounters.TOTAL_UNIQUE_MOVIES);
                    counter.increment(1);
                    
                } else {
                    movieRow.setTitle("Row record type of R or I is incomplete");              
                }
           }
            
           movieRow.setUniqueUsers(valIterator);
           movieRow.setRatings(valIterator);
           movieRow.setRatingsAverage(sumRatings/valIterator);
            
           // set and write the reducer output
           OutputRow.set(movieRow.toString());
           context.write(key, OutputRow);
            
            
    }
}
