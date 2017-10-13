/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw3;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

// import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author Ethan
 */
public class RatingDistribution {
    
    public static void main(String args[]) 
            throws IOException, ClassNotFoundException, InterruptedException {
        
        // create a job instance
        Job ratingCountJob = 
                Job.getInstance(new Configuration(), "RatingCount");
        
        // HDFS input path
        TextInputFormat.addInputPath(ratingCountJob, new Path(args[0]));
                
        // input data format
        ratingCountJob.setInputFormatClass(TextInputFormat.class);
        
        // mapper reducer classes
        ratingCountJob.setMapperClass(RatingCountMapper.class);
        ratingCountJob.setReducerClass(RatingCountReducer.class);
        
        // jar file
        ratingCountJob.setJarByClass(RatingDistribution.class);
        
        // HDFS output path
        TextOutputFormat.setOutputPath(ratingCountJob, new Path(args[1]));
        
        // output data format
        ratingCountJob.setOutputFormatClass(TextOutputFormat.class);
        
        // set output key and value classes
        ratingCountJob.setOutputKeyClass(Text.class);
        ratingCountJob.setOutputValueClass(IntWritable.class);
    
        // submit the job
        ratingCountJob.waitForCompletion(true);
    }
    
}
