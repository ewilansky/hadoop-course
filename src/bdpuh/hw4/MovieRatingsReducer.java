/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw4;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ethanw
 */
public class MovieRatingsReducer 
            extends Reducer<CompositeKeyWritable, Text, NullWritable, Text> {
    
    StringBuilder reduceValueBuilder = new StringBuilder("");
    NullWritable nullWritableKey = NullWritable.get();
    Text reduceOutputValue = new Text("");
    String strSeparator = ",";

    private StringBuilder buildOutputValue(CompositeKeyWritable key,
                    StringBuilder reduceValueBuilder, Text value) {

        if (key.getsourceIndex() == 1) {           
            // Insert the joinKey (movieId) to beginning of the stringBuilder
            reduceValueBuilder.append(key.getjoinKey()).append(strSeparator);
            // String arrRatingsAttributes[] = value.toString().split("\t");
            reduceValueBuilder.append(value.toString()).append(strSeparator);

        } else {
            // Join movie info with ratings and summary info (1..1 on join key)
            // Salary data; Just append the salary, drop the effective-to-date
            String arrSalAttributes[] = value.toString().split(",");
            reduceValueBuilder.append(arrSalAttributes[0].toString()).append(
                            strSeparator);
        } 


        return reduceValueBuilder;
    }

    @Override
    public void reduce(CompositeKeyWritable key, Iterable<Text> values,
                    Context context) throws IOException, InterruptedException {

            // Iterate through values; First set is csv of employee data
            // second set is salary data; The data is already ordered
            // by virtue of secondary sort; Append each value;
            for (Text value : values) {
                    buildOutputValue(key, reduceValueBuilder, value);
            }

            // Drop last comma, set value, and emit output
            if (reduceValueBuilder.length() > 1) {

                    reduceValueBuilder.setLength(reduceValueBuilder.length() - 1);
                    // Emit output
                    reduceOutputValue.set(reduceValueBuilder.toString());
                    context.write(nullWritableKey, reduceOutputValue);
            } else {
                    System.out.println("Key=" + key.getjoinKey() + "src="
                                    + key.getsourceIndex());

            }

            // Reset variables
            reduceValueBuilder.setLength(0);
            reduceOutputValue.set("");

    }
    
}
