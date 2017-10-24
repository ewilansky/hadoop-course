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
    StringJoiner joiner = new StringJoiner(",");
    Text dataRow = new Text();
    
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
      
        // gets a line of text 
        String row = value.toString();
        
        // System.out.println("in map row value is: " + row);
        
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        
        if (fileName.endsWith(".data")) {
            // split the tab delimited file
            String[] cols = row.split("\t");
            movieIdKey.set(Integer.parseInt(cols[1]));
            joiner.add(cols[0]).add(cols[2]);
        } else {
            // split pipe delimited .item file
            String[] cols = row.split("|");
            movieIdKey.set(Integer.parseInt(cols[0]));
            joiner.add(cols[0]).add(cols[1]).add(cols[2]).add(cols[3]);
        }
       
        dataRow.set(joiner.toString());
        context.write(movieIdKey, dataRow); 
    }
}
