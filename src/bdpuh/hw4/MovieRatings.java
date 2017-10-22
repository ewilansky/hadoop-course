/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw4;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
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

        System.out.println("in MovieRatings run method");
        
        // create and configure a job instance
        Job job = 
            Job.getInstance(new Configuration(), "MovieRatings");
            
        Configuration conf = job.getConfiguration();
        job.setJarByClass(MovieRatings.class);
        job.setJobName("MovieRatingsJoin");
        
        // get the input files from HDFS
        TextInputFormat.addInputPath(job, new Path(args[0]));
        
       // input data format
        job.setInputFormatClass(TextInputFormat.class);
        
        // Set sourceIndex for input files;
        // sourceIndex is an attribute of the compositeKey,
        // to drive order, and reference source
        
        // might set this index label in mapper??
        // conf.setInt("part-e", 1);// Set Employee file to 1
        
        conf.setInt("u.item", 1);// Set Current movie data file to 1

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MovieRatingsMapper.class);
        job.setCombinerClass(MovieRatingsCombiner.class);
        job.setMapOutputKeyClass(CompositeKeyWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(Partitioner.class);
        job.setSortComparatorClass(SortingComparator.class);
        job.setGroupingComparatorClass(GroupingComparator.class);

        // 2 reducers per assignment requirements
        job.setNumReduceTasks(2);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        // }}

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String args[]) 
                    throws Exception {
        
        System.out.println("in MovieRatings main method");
        
        int exitCode = ToolRunner.run(new Configuration(), new MovieRatings(),
                        args);
        
        System.exit(exitCode);
    }

}