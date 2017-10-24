/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw4;

import java.io.IOException;
import java.util.StringJoiner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
            sb.append(cols[0]).append(",").append(cols[2]);
        } else {
            // split pipe delimited .item file
            String[] cols = row.split("|");
            movieIdKey.set(Integer.parseInt(cols[0]));
            sb.append(cols[0]).append(",")
                    .append(cols[1]).append(",")
                    .append(cols[2]).append(",")
                    .append(cols[3]);
        }
       
        dataRow.set(sb.toString());
        context.write(movieIdKey, dataRow); 
        
        // efficiently clears the string builder between row processing
        sb.delete(0, sb.length());
    }
}
