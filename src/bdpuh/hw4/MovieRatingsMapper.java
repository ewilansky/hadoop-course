/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 * Some methods adapted from: MapperRSJ by Anagha Khanolkar
 */
package bdpuh.hw4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author ethanw
 */
public class MovieRatingsMapper 
            extends Mapper<LongWritable, Text, CompositeKeyWritable, Text> {
    
        CompositeKeyWritable compositeKey = new CompositeKeyWritable();
	Text txtValue = new Text("");
	int intSrcIndex = 0;
	StringBuilder strMapValueBuilder = new StringBuilder("");
	
        List<Integer> fileColumns = new ArrayList<Integer>();

	@Override
	protected void setup(Context context) 
                throws IOException, InterruptedException {

            // Get the source index; (ratings data = 1, movie data = 2)
            // Added as configuration in driver
            FileSplit fsFileSplit = (FileSplit) context.getInputSplit();

            intSrcIndex = Integer.parseInt(context.getConfiguration().get(
                            fsFileSplit.getPath().getName()));

            // Initialize the list of fields to emit as output based on
            // intSrcIndex (1=ratings data, 2=movie data)
            if (intSrcIndex == 1) // ratings data
            {
                fileColumns.add(0); // UserId
                fileColumns.add(2); // Rating
            } else { // movie data
                fileColumns.add(1); // MovieTitle
                fileColumns.add(2); // ReleaseData
                fileColumns.add(3); // IMDb_URL
            }
            
            System.out.println("In mapper");
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

            if (value.toString().length() > 0) {
                
                // string splits for two different delimeter types
                // tabs in Movie Ratings and pipes in Movie Data
                String arrEntityAttributes[] = 
                        intSrcIndex == 1 ? value.toString().split("\t") :
                        value.toString().split("|");
                                
                compositeKey.setjoinKey(arrEntityAttributes[0].toString());
                compositeKey.setsourceIndex(intSrcIndex);
                txtValue.set(buildMapValue(arrEntityAttributes));
                
                System.out.println("composite key: " + compositeKey);
                System.out.println("alue: " + txtValue);

                System.out.printf("compositeKey: %s", compositeKey);
                System.out.printf("txtValue: %s", txtValue);
                context.write(compositeKey, txtValue);
            }

	}
        
       // returns csv list of values to emit based on data entity
        private String buildMapValue(String arrEntityAttributesList[]) {

            strMapValueBuilder.setLength(0);// Initialize

            // Build list of attributes to output based on source
            // movie ratings or movie data
            for (int i = 1; i < arrEntityAttributesList.length; i++) {
                // If the field is in the list of required output
                // append to stringbuilder
                if (fileColumns.contains(i)) {
                        strMapValueBuilder.append(arrEntityAttributesList[i]).append(
                                        ",");
                }
            }
            if (strMapValueBuilder.length() > 0) {
                    // Drop last comma
                    strMapValueBuilder.setLength(strMapValueBuilder.length() - 1);
            }

            return strMapValueBuilder.toString();
	}
    
}
