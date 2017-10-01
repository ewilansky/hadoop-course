/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bdpuh.hw2;

// local path and file imports
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;


// hdfs path and file imports
import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

/**
 *
 * @author Ethan WIlansky
 */
public class ParallelLocalHdfsCopy {
    //static {
        // Configuration.addDefaultResource("hadoop/config/demo/my-app-conf.xml");
    // }

    public static void main(String[] args) throws IOException {
                
        if (args.length < 3) {
            System.out.println("You must specify three arguments, local "
                    + "source directory, hdfs target directory and the number "
                    + "of threads to use for copying");
            
            System.exit(-1);
        }
        
        String srcDir = args[0];
        String dstDir = args[1];
        int numberThreads = 0;
        
        try
        {
          // the String to int conversion happens here
          numberThreads = Integer.parseInt(args[2].trim());
        }
        catch (NumberFormatException nfe)
        {
          System.out.println("NumberFormatException: " + nfe.getMessage());
        }
        
        // start HDFS section
        Configuration config = new Configuration();
        // verify that the HDFS destination directory does not exist
        // message must be: The destination directory already exists. Please
        // delete before running this program.
        Path dstDirPath = new Path(dstDir);
        
        // get a copy of the HDFS file system object
        FileSystem fileSystem = null;
        try {
            // get a file system object
            fileSystem = FileSystem.get(config);
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
            System.err.format("Destination directory already exists. Please\n" +
                "delete before running the program.", dstDir);
            System.exit(-1);
        }
        
        // Close the FileSystem
        try {
            fileSystem.close();
        } catch (IOException ex) {
            System.err.format("Unable to close the HDFS file system: %s",  ex.getMessage());
        }
        
        System.out.printf("Created directory: %s sucessfully, status: %b", dstDir, status);

         File dir = new File(srcDir);
         File[] dirList = dir.listFiles();
         if (dirList != null && dir.isDirectory()) {
             for (File file : dirList) {
               CompressToHdfs(config, file, dstDir);
            }
          } else {
             System.err.printf("Failure in geting a directory listing for source directory %s", srcDir);
             System.exit(-1);
          }
             
       return;
    }

    private static void CompressToHdfs(Configuration config, File file, String dstDir) {
         // copy files from local in parallel and compress concurrently
        String srcFile = file.getAbsolutePath();
        
        Path srcFilePath = new Path(srcFile);
        // Path dstFilePath = new Path(dstDir + "/file.txt");

        // Get a copy of FileSystem Object
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(config);
        } catch (IOException ex) {
            System.err.printf("Unable to get file system config: %s", ex.getMessage());
        }
 
        // This stream will be used  to open a local file for reading
        InputStream fsInputStream = null;
        
        try {
             //Input stream for the file in local file system to be written to HDFS
            fsInputStream = new BufferedInputStream(new FileInputStream(srcFile));
            // fSDataInputStream = fileSystem.open(dstFilePath);
        } catch (IOException ex) {
            System.err.printf("Unable to open an input stream to file: %s", ex.getMessage());
        }

        // Open a File for Writing .gz file
        System.out.println(srcFilePath.getName());
        String srcFileName = srcFilePath.getName();
        Path compressedFileToWrite = new Path(dstDir + "/" + srcFileName + ".gz");
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = fileSystem.create(compressedFileToWrite);
        } catch (IOException ex) {
            System.err.printf("Unable to compress %s. Error; %s", srcFileName, ex.getMessage());
        }
        
        // Get Compressed FileOutputStream
        CompressionCodec compressionCodec = new GzipCodec();
        CompressionOutputStream compressedOutputStream = null;
        try {
            compressedOutputStream =
                    compressionCodec.createOutputStream(fsDataOutputStream);
        } catch (IOException ex) {
            System.err.printf("Unable to create compression output stream %s", ex.getMessage());
        }

        try {
            IOUtils.copyBytes(fsInputStream, compressedOutputStream, config);
        } catch (IOException ex) {
            System.err.printf("Unable to copy bytes: $s", ex.getMessage());
        }

        // Close streams
        try {
            fsInputStream.close();
            fsDataOutputStream.close();
            compressedOutputStream.close();
            fileSystem.close();
        } catch (IOException ex) {
            System.err.printf("Unable to close all resources %s", ex.getMessage());
        }

        System.out.printf("Compressed file %s successfully", file.getAbsolutePath());
       
    }
}
