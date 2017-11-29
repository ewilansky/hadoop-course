/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bdpuh.oauth;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author ethanw
 */
public class Client {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hdserver.local:50070");
        FileSystem fs = FileSystem.get(conf);
        
        FileStatus[] fsStatus = fs.listStatus(new Path("/movie-and-ratings"));
        
        for(int i = 0; i < fsStatus.length; i++) {
            System.out.println(fsStatus[i].getPath().toString());
        }
    }
    
}
