/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoop.course.assignment3.bdpuh.hw2;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author hdadmin
 */
public class ParallelLocalHdfsCopy {
    //static {
        // Configuration.addDefaultResource("hadoop/config/demo/my-app-conf.xml");
    // }

    public static void main(String[] args) throws IOException {
        /* for (String s: args) {
        System.out.println("arg ->" + s);
        } */
        
        if (args.length < 3) {
            System.out.println("You must specify three arguments, local "
                    + "source directory, hdfs target directory and the number "
                    + "of threads to use for copying");
        }
        
        // verify that the first argument is an absolute path 
        
        // verify that the absolute path exists on thhe local file system
        // mesage must be: The source directory does not exist.
        
        // verify that the HDFS destination directory does not exist
        // message must be: The destination directory already exists. Please
        // delete before running this program.
        
        // if you get here, create the hdfs directory
        
        // copy files in parallel and compress concurrently
        
        Configuration conf = new Configuration();
        // dumps entire Hadoopp configuration as single string of key value pairs
        Configuration.dumpConfiguration(conf, new PrintWriter(System.out));
        
        //        for(Map.Entry<String, String> entry: conf) {
        //            System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
        //       }
        //        
        //       System.out.println("fs.default.name is: " + conf.get("fs.default.name")); 
        //       System.out.println("fs.defaultFS is " + conf.get("fs.defaultFS"));
       
       return;
    }
}
