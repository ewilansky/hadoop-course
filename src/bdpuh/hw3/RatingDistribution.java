/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw3;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

// import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 *
 * @author hdadmin
 */
public class RatingDistribution {
    
    public static void main(String args[]) throws IOException {
        Job ratingCountJob = null;
        
        ratingCountJob = Job.getInstance(new Configuration(), "RatingCount");
        
        // HDFS input path
        TextInputFormat.addInputPath(ratingCountJob, new Path(args[0]));
                
        // input data format
        ratingCountJob.setInputFormatClass(TextInputFormat.class);
    
    }
    
}
