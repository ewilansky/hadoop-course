/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw9;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import static org.apache.hadoop.hbase.util.Bytes.*;

/**
 *
 * @author ethanw
 */
public class UserAdmin {
    public static void main(String args[]) 
            throws IOException, ClassNotFoundException, InterruptedException {
        

        // setup hbase config
        Configuration conf = HBaseConfiguration.create();
        
        // get the table containing where records will be inserted
        HTable hTable = new HTable(conf, "User");
        
        // add a row to the table
        Put put01 = new Put(toBytes("row01"));

    }           
    
    
}
