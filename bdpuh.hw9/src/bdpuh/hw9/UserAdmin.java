/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw9;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import static org.apache.hadoop.hbase.util.Bytes.*;

/**
 *
 * @author ethanw
 */
public class UserAdmin {
    
    static Connection connection = null;
    static Configuration config = null;
    static Table table = null;
    
    
    public static void main(String args[]) 
            throws IOException, ClassNotFoundException, InterruptedException {
        
        if (args.length == 0) {
            System.out.println("You must include arguments to run this command");
            System.exit(1);
        } 
        
        String verb = args[0];

        // setup hbase config
        config = HBaseConfiguration.create();
        
        // java UserAdmin add kss k.s@gmail.com mypasswd
        // married 1970/06/03 “favorite color” “red”
        if(args[0].equalsIgnoreCase("add")) 
        {
            // verify that there are a total of 8 arguments
            if (args.length == 8)
            {            
                add(config, args);
            }
            else
            {
                System.out.println("The Add request requires the following "
                    + "seven arguments to be specified : "
                    + "rowId, emailid, password, marriage status, date of birth, "
                    + "security question and security answer");
            }
        }

    }           

    private static void add(Configuration conf, String[] args) throws IOException {
        
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
        
        String rowId = args[1];
        
        HashMap<String, String> credCols = new HashMap();
        
        credCols.put("email", args[2]);
        credCols.put("password", args[3]);
        
        HashMap<String, String> prefCols = new HashMap();
        
        String marriageStatus = args[4].trim().toUpperCase();
        try {
            // verify valid enum for marriage status
            MarriageStatus.valueOf(marriageStatus);
        } catch (IllegalArgumentException ia) {
            System.out.println("marriage status value specified is not allowed."
                    + "it must be married, single, divorced or widowed.");
        }
        
        // if we get this far, the fourth arg is in the right format, but 
        // using the string manipulation to make column value consistent
        prefCols.put("status", marriageStatus);
        
        try {
            formatter.parse(args[5]);
        } catch (ParseException pe) {
            System.out.println("date of birth is not in the right format."
                    + "it must be yyyy/MM/dd");
        }
 
        // if we get this far, the fifth arg is in the right format
        prefCols.put("date_of_birth", args[5]);
        
        
        prefCols.put("security_question", args[6]);
        prefCols.put("security_answer", args[7]);


        // add a row to the table
        Put row = new Put(toBytes(rowId)); 
        
        // write the columns to the creds column family
        for (String key : credCols.keySet()) {
            String val = credCols.get(key);       
            row.addColumn(toBytes("creds"), toBytes(key), toBytes(val));
        }
        
        // write the columns to the prefs column family
        for (String key : prefCols.keySet()) {
            String val = prefCols.get(key);
            row.addColumn(toBytes("prefs"), toBytes(key), toBytes(val));
        }
        
        // get a table and add the row
        try 
        {            
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf("User"));
            table.put(row);
        }    
        finally
        {
            table.close();
            connection.close();
        }
        
    }
    
    
}
