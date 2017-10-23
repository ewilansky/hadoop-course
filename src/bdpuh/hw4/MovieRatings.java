/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

        System.out.println("1. Entered MovieRatings run method");
        
        // create and configure a job instance
        Job job = 
            Job.getInstance(new Configuration(), "MovieRatings");
            
        Configuration conf = job.getConfiguration();
        job.setJarByClass(MovieRatings.class);
        job.setJobName("MovieRatingsJoin");
        

        List<Path> inputhPaths = new ArrayList<>();

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] listStatus = fs.globStatus(new Path(args[0] + "/*.data"));
        for (FileStatus fstat : listStatus) {
            inputhPaths.add(fstat.getPath());
        }

        FileInputFormat.setInputPaths(job,
                (Path[]) inputhPaths.toArray(new Path[inputhPaths.size()]));
        
        
        // get the input files from HDFS
        // TextInputFormat.addInputPath(job, new Path(args[0]));
        
        System.out.println("2. Added input path of: " + args[0]);
        
       // input data format
        job.setInputFormatClass(TextInputFormat.class);
        
        // Set sourceIndex for input files;
        // sourceIndex is an attribute of the compositeKey,
        // to drive order, and reference source
        
        // might set this index label in mapper??
        // conf.setInt("part-e", 1);// Set Employee file to 1
        
        conf.setInt("u.item", 1);// Set Current movie data file to 1
        
//        conf.setInt("u1.data", 2);
//        conf.setInt("u2.data", 3);
//        conf.setInt("u3.data", 4);
//        conf.setInt("u4.data", 5);
//        conf.setInt("u5.data", 6);
        
        System.out.println("Set integer tag on u.item");

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MovieRatingsMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        System.out.println("Set Mapper class, output key and output value classes");
        
        // COMBINERS THROWING ERRORS, ADD BACK IN LATER
        // job.setCombinerClass(MovieRatingsCombiner.class);
        // job.setCombinerKeyGroupingComparatorClass(cls);

        // System.out.println("Getting ready to set partitioner class");
        // job.setPartitionerClass(Partitioner.class);
//        System.out.println("Finished setting partitioner class");
//        System.out.println("Getting ready to set sort class");
//        job.setSortComparatorClass(SortingComparator.class);
//        System.out.println("Finished setting sort class");
//        System.out.println("Getting ready to set grouping class");
//        job.setGroupingComparatorClass(GroupingComparator.class);
//        System.out.println("Finished setting grouping class");

        // 2 reducers per assignment requirements
        job.setNumReduceTasks(1);
        job.setReducerClass(MovieRatingsReducer.class);
        // System.out.println("Finished setting reducer class");
        
        // System.out.println("Getting ready to set job output key class");
        // job.setOutputKeyClass(NullWritable.class);
//        System.out.println("set job output key class");
//        System.out.println("Getting ready to set job output value class");
//        job.setOutputValueClass(Text.class);
//        System.out.println("set job output value class");

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