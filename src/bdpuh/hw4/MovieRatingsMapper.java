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
    
    protected void setup(Context context) throws IOException, InterruptedException {
        fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
       
        // gets a line of text 
        String row = value.toString();
        
        System.out.println("Row value is: " + row);
        
        if (fileName.endsWith(".data.gz")) {
            // split the tab delimited file
            String[] cols = row.split("\t");
            movieIdKey.set(Integer.parseInt(cols[1]));       
            sb.append("R|").append(cols[0]).append("|").append(cols[2]);
        } else {
   
            // split pipe delimited .item file
            String[] cols = row.split("\\|");
            
            System.out.println("column 0: " + cols[0]);
            System.out.println("column 1: " + cols[1]);
            
            Integer integerKey = 0; 
            
            try {
                integerKey = Integer.parseInt(cols[0]);
            } catch (NumberFormatException ex) {
                integerKey = 99999;
            }
            
            movieIdKey.set(integerKey);
            
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
}
