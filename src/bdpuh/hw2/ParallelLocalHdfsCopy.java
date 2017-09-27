/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bdpuh.hw2;

// local path and file imports
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.NoSuchFileException;
// import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

// hdfs path and file imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
            
            System.exit(-1);
        }
        
        String sourceDir = args[0];
        String targetDir = args[1];
        int numberThreads = 0;
        
        try
        {
          // the String to int conversion happens here
          numberThreads = Integer.parseInt(args[3].trim());
        }
        catch (NumberFormatException nfe)
        {
          System.out.println("NumberFormatException: " + nfe.getMessage());
          System.exit(-1);
        }
        
        //// Local File IO section
        // verify that the first argument is an absolute path 
        // assuming linux path syntax
        
        // verify that the absolute path exists on the local file system
        // mesage must be: The source directory does not exist
        
        // Converts the input string to a Path object.
        java.nio.file.Path inputPath = Paths.get(sourceDir);
        
        java.nio.file.Path path = null;
        
        try {
            path = inputPath.toRealPath();
        } catch (NoSuchFileException x) {
            System.err.format("The path: %s is not a directory%n", sourceDir);
            System.exit(-1);
        } catch (IOException x) {
            System.err.format("%s%n", x);
            System.exit(-1);
        }
        
        // verify that the value entered is a directory
        File pathEntered = new File(path.toString());
        if (!pathEntered.isDirectory()) {
            System.err.format(
                    "The path entered: %s, points to a file, not a directory", sourceDir);
            System.exit(-1);
        } else {
            System.out.printf("Good path, start copying from %s", sourceDir);
            System.exit(-1);
        }
        
        /// end file IO section
        
        //// start HDFS section
        Configuration config = new Configuration();
        // verify that the HDFS destination directory does not exist
        // message must be: The destination directory already exists. Please
        // delete before running this program.
        Path dir = new Path(targetDir);
        
        // get a copy of the HDFS file system object
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(config);
        } catch (IOException ex) {
            System.err.format("Can't get HDFS file system object %s", ex.getMessage());
            System.exit(-1);
        }
        
        // Create an HDFS Directory
        boolean status = false;
        try {
            status = fileSystem.mkdirs(dir);
        } catch (IOException ex) {
            System.err.format("Destination directory already exists. Please\n" +
                "delete before running the program.", ex.getMessage());
        }
        
        // Close the FileSystem
        try {
            fileSystem.close();
        } catch (IOException ex) {
            System.err.format("Unable to close the HDFS file system: %s", 
                    ex.getMessage());
        }

        // copy files in parallel and compress concurrently
               
       return;
    }
}
