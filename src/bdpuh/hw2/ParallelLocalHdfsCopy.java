/*
* EN.605.788.81.FA17 Big Data Processing Using Hadoop
* Student/Author: Ethan Wilansky
*/
package bdpuh.hw2;

// local file system related imports
import java.io.File;
import java.io.IOException;

// multi-threading imports
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// hdfs path and file imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author Ethan WIlansky
 */
public class ParallelLocalHdfsCopy {

    public static void main(String[] args) throws IOException {
                
        // <editor-fold desc="Arguments region" defaultstate="collapsed">
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
          // String to int conversion
          numberThreads = Integer.parseInt(args[2].trim());
        }
        catch (NumberFormatException nfe)
        {
          System.out.println("NumberFormatException: " + nfe.getMessage());
        }
        // </editor-fold>
        
        // <editor-fold desc="Hadoop region">
        
        // create global Hadoop configuration object
        Configuration config = new Configuration();
        
        // MakeHdfsDirectory(dstDir, config);
        
        // setup a single threaded executor for creating the HDFS director
        ExecutorService directoryExecutor = Executors.newFixedThreadPool(1);
        
        Runnable dirWorker = new MakeHdfsDirThread(config, dstDir);
        directoryExecutor.execute(dirWorker);
        ThreadShutdown(directoryExecutor);
        
        
        // setup a thread executor service for compression 
        ExecutorService compressionExecutor = Executors.newFixedThreadPool(numberThreads);
        
         File dir = new File(srcDir);
         File[] dirList = dir.listFiles();
         if (dirList != null && dir.isDirectory()) {
             for (int fileCmd = 0; fileCmd < dirList.length; fileCmd++) {
               File file = dirList[fileCmd];
               // Run compression routine multi-threaded               
               Runnable compressionWorker = 
                     new CompressToHdfsThread(fileCmd, config, file, dstDir);
               compressionExecutor.execute(compressionWorker);
            }
            ThreadShutdown(compressionExecutor);            
          } else {
             System.err.printf("Failure in geting a directory listing for source directory %s", srcDir);
             System.exit(-1);
          }
         // </editor-fold>
         
       //</editor-fold>
             
         return;
    }

    private static void ThreadShutdown(ExecutorService executor) {
        // <editor-fold desc="thread termination logic" defaultstate="collapsed">
        try {
            System.out.println("attempt to shutdown executor");
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            System.err.println("tasks interrupted");
        }
        finally {
            if (!executor.isTerminated()) {
                System.err.println("cancel non-finished tasks");
            }
            executor.shutdownNow();
            System.out.println("shutdown finished");
        }
    }

    // <editor-fold desc="Privates" defaultstate="collapsed">
    private static void MakeHdfsDirectory(String dstDir, Configuration config) throws IllegalArgumentException {
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
    
// </editor-fold>
}
