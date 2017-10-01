/*
* EN.605.788.81.FA17 Big Data Processing Using Hadoop
* Student/Author: Ethan Wilansky
*/
package bdpuh.hw2;

// Local filesytem imports
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

/**
 *
 * @author Ethan Wilansky
 */
public class CompressToHdfsThread implements Runnable {
    
    private String command;
    private Configuration config;
    private File file;
    private String dstDir;
    
    public CompressToHdfsThread(Integer command, Configuration config, File file, String dstDir) {
        this.command = command.toString();
        this.config = config;
        this.file= file;
        this.dstDir= dstDir;
    }
    
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + " Start. File compress command = " + command);
        processCommand();
        System.out.println(Thread.currentThread().getName() + " End.\n");
    }
    
    private void processCommand() {
        CompressToHdfs(config, file, dstDir);
    }
    
    private static void CompressToHdfs(Configuration config, File file, String dstDir) {
        // copy files from local in parallel and compress concurrently
        String srcFile = file.getAbsolutePath();
        
        Path srcFilePath = new Path(srcFile);
        
        // Get a copy of FileSystem Object
        FileSystem fileSystem = null;
        try {
            // Thread safe for file system. New instance for each thread and operation
            fileSystem = FileSystem.newInstance(config);
        } catch (IOException ex) {
            System.err.printf("Unable to get file system config: %s", ex.getMessage());
        }
        
        // This stream will be used  to open a local file for reading
        InputStream fsInputStream = null;
        
        try {
            // Input stream for the file in local file system to be written to HDFS
            fsInputStream = new BufferedInputStream(new FileInputStream(srcFile));
        } catch (IOException ex) {
            System.err.printf("Unable to open an input stream to file: %s", ex.getMessage());
        }
        
        // Open a File for Writing .gz file
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
            compressedOutputStream
                    = compressionCodec.createOutputStream(fsDataOutputStream);
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
            if (fsInputStream != null) fsInputStream.close();
            if (fsDataOutputStream != null) fsDataOutputStream.close();
            if (compressedOutputStream != null) compressedOutputStream.close();
            if (fileSystem != null) fileSystem.close();
        } catch (IOException ex) {
            System.err.printf("Unable to close all resources %s", ex.getMessage());
        }
        
        System.out.printf("Compressed file %s successfully\n", file.getAbsolutePath());
    }
    
    @Override
    public String toString() {
        return this.command;
    }
    
}
