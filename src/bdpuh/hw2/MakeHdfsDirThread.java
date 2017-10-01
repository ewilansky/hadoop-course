/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author Ethan Wilansky
 */
public class MakeHdfsDirThread implements Runnable {
    
    private Configuration config;
    private String dstDir;
    
    public MakeHdfsDirThread(Configuration config, String dstDir) {
        this.config = config;
        this.dstDir = dstDir;
    }
    
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + " Start. Making HDFS directory command");
        processCommand();
        System.out.println(Thread.currentThread().getName() + " End.\n");
        return;
    }
    
    private void processCommand() {
        MakeHdfsDirectory(dstDir, config);
    } 
    
     private static void MakeHdfsDirectory(String dstDir, Configuration config) {
        // verify that the HDFS destination directory does not exist
        // message must be: The destination directory already exists. Please
        // delete before running this program.
        Path dstDirPath = new Path(dstDir);
        
        // get a copy of the HDFS file system object
        FileSystem fileSystem = null;
        try {
            // get a file system object
            fileSystem = FileSystem.newInstance(config);
        } catch (IOException ex) {
            System.err.format("Can't get HDFS file system object %s", ex.getMessage());
        }
        
        // Create an HDFS Directory
        boolean status = false;
        try {
            status = fileSystem.mkdirs(dstDirPath);
        } catch (IOException ex) {
            System.err.format("Unable to create destination directory %s. The error was %s", dstDir, ex.getMessage());
        }
        
        //  False status likely means the directory already exists.
        if (status == false) {
            System.err.format("Destination directory already exists. Please delete before running the program.", dstDir);
            System.exit(-1);
        }
        
        // Close the FileSystem
        try {
            fileSystem.close();
        } catch (IOException ex) {
            System.err.format("Unable to close the HDFS file system: %s",  ex.getMessage());
        }
        
        System.out.printf("Created directory: %s sucessfully. Status is: %b\n", dstDir, status);
    }
     
     @Override
     public String toString() {
         return "Make Hdfs directory command";
     }
    
    
}
