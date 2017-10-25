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
            extends Reducer<IntWritable, Text, IntWritable, Text> {

    int i = 0;
    IntWritable count = new IntWritable();
    StringBuilder sb = new StringBuilder();
    Text OutputRow = new Text();
    int valIterator = 0;
    int sumRatings = 0;
        
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values,
        Context context) 
            throws IOException, InterruptedException {
        
       // combine the ratings files here      
        valIterator = 0;
        sumRatings = 0;

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
            }
       }

       int averageRatings = sumRatings/valIterator;
       sb
        .append(valIterator)
        .append("|")
        .append(valIterator)
        .append("|")
        .append(averageRatings);

       System.out.println("Combiner row: " + sb.toString());
       // set and write the reducer output
       OutputRow.set(sb.toString());
       // efficiently clears the string builder between row processing
       sb.delete(0, sb.length());

       context.write(key, OutputRow);      
    }
}
