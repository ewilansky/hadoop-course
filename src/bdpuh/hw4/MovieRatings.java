/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author ethanw
 */
public class MovieRatings 
            extends Configured implements Tool {
   
    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.printf(
                "Two parameters are required: <input dir> <output dir>\n");
            return -1;
        }
        
        // create and configure a job instance
        Job job = 
            Job.getInstance(new Configuration(), "MovieRatings");
        
        
        Configuration conf = job.getConfiguration();  
        
        // run job locally for debugging only!!
        // conf.set("mapreduce.framework.name", "local");
        
        // enable debug logging for map and reduce tasks in this job
        conf.set("mapreduce.map.log.level", "DEBUG");
        conf.set("mapreduce.reduce.log.level", "DEBUG");
          
        conf.setInt("mapreduce.job.reduces", 2);
      
        // set output compression
        conf.setBoolean("mapreduce.output.compress", true);
        conf.setStrings("mapreduce.output.compress.GzipCodec");
        
        // set intermediate mapper/reducer compression
        conf.setBoolean("mapreduce.compress.map.output", true);
        conf.setStrings("mapreduce.map.output.compression.codec", 
                "org.apache.hadoop.io.compress.GzipCodec");
        
        job.setJarByClass(MovieRatings.class);
        job.setJobName("MovieRatingsJoin");

        // HDFS input path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
       // input data format
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MovieRatingsMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        // TODO ADD fix combiner if time allows
        // job.setCombinerClass(MovieRatingsCombiner.class);

        job.setReducerClass(MovieRatingsReducer.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String args[]) 
                    throws Exception {
        
        int exitCode = ToolRunner.run(new Configuration(), new MovieRatings(),
                        args);
        
        System.exit(exitCode);
    }
}