/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw4;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.text.Normalizer;
import java.util.regex.Pattern;

/**
 *
 * @author Ethan
 */
public class MovieRatingsMapper 
            extends Mapper<LongWritable, Text, IntWritable, Text> {
           
    IntWritable movieIdKey = new IntWritable();
    Text dataRow = new Text();
    String fileName = "";
    StringBuilder sb = new StringBuilder();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
       
        // gets a line of text 
        String row = value.toString();
        
        if (fileName.endsWith(".data")) {
            // split the tab delimited file
            String[] cols = row.split("\t");
            movieIdKey.set(Integer.parseInt(cols[1]));       
            sb.append("R|").append(cols[0]).append("|").append(cols[2]);
        } else {
            // scrub string of accented characters, see deAccent() comment 
            // preceding the method.
            // String scrubedRow = deAccent(row);
   
            // split pipe delimited .item file
            String[] cols = row.split("\\|");
            movieIdKey.set(Integer.parseInt(cols[0]));
            sb.append("I|").append(cols[1]).append("|")
                .append(cols[2]).append("|")
                .append(cols[4]);
        }
       
        dataRow.set(sb.toString());
        
        // efficiently clears the string builder between row processing
        sb.delete(0, sb.length());

        context.write(movieIdKey, dataRow); 
        
        // count total records
        Counter counter = context.getCounter(MovieRatingCounters.TOTAL_RECORDS);
        counter.increment(1);
       
    }
    
    // some movie titles have accented strings that are not handled properly
    // by the StringBuilder. Removing the accented strings improves the output
    // for example, see record: 1623 or 1633 in the u.item data set
//    public String deAccent(String str) {
//        String nfdNormalizedString = 
//                Normalizer.normalize(str, Normalizer.Form.NFD); 
//        
//        Pattern pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
//        
//        return pattern.matcher(nfdNormalizedString).replaceAll("");
//    }
}
