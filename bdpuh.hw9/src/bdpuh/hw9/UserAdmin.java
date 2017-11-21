/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw9;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import static org.apache.hadoop.hbase.util.Bytes.*;
import org.joda.time.DateTime;

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
        
        String verb = args[0].toLowerCase();

        // setup hbase config
        config = HBaseConfiguration.create();
        
        switch(verb) {
            case "add": {
                // verify that there are a total of 8 arguments
                if (args.length == 8) {            
                    add(config, args);
                } else {
                    System.out.println("The Add request requires the following "
                        + "seven arguments to be specified : "
                        + "rowId, emailid, password, marriage status, date of birth, "
                        + "security question and security answer");
                }
                
                break;            
            }
            case "delete": {
                if (args.length == 2) {
                    delete(config, args);
                } else {
                    System.out.println("The Delete request requires the following "
                        + "argument to be specified : rowId");
                }
                
                break;            
            }
            case "show": {
                if (args.length == 2) {
                    show(config, args);                
                } else {
                    System.out.println("The Show request requires the following "
                        + "argument to be specified : rowId");
                }
                
                break;            
            }
            case "listall": {
                if (args.length == 1) {
                
                } else {
                    System.out.println("The Listall request takes no "
                            + "additional arguments");
                }
                
                break;
            }
            case "login": {
                if (args.length == 4) {
                
                } else {
                    System.out.println("The Login request requires the following "
                        + "3 arguments to be specified : "
                        + "rowId, password, ip address");
                }
                
                break;
            
            }
            default: {
                System.out.println(
                        "Valid verbs/actions are: add, delete, show, listall, login");
                
                break;
            
            }
                
        }
    }           

    private static void add(Configuration config, String[] args) 
            throws IOException {
        
       
        String rowId = args[1];
        
        HashMap<String, String> credCols = SetupCredsMap(args[2], args[3]);
        
        HashMap<String, String> prefCols = SetupPrefsMap(
                args[4], args[5], args[6], args[7]);

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
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf("User"));
            table.put(row);
        }    
        finally
        {
            table.close();
            connection.close();
        }        
    }
    
    private static HashMap<String, String> SetupLastLoginMap(
            String ipAddress, DateTime now, Boolean success) {
    
        HashMap<String, String> loginCols = new HashMap<>();
        
        // TBD: fill this in
        
        return loginCols;
    
    }
    

    private static HashMap<String, String> SetupPrefsMap(
            String mStatus, String dob, String secQuestion, String secAnswer) {        
        
        
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
        HashMap<String, String> prefCols = new HashMap<>();
        String marriageStatus = mStatus.trim().toUpperCase();
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
            formatter.parse(dob);
        } catch (ParseException pe) {
            System.out.println("date of birth is not in the right format."
                    + "it must be yyyy/MM/dd");
        }
        // if we get this far, the fifth arg is in the right format
        prefCols.put("date_of_birth", dob);
        prefCols.put("security_question", secQuestion);
        prefCols.put("security_answer", secAnswer);
        return prefCols;
    }

    private static HashMap<String, String> SetupCredsMap(
            String email, String password) {
        
        HashMap<String, String> credCols = new HashMap<>();
        credCols.put("email", email);
        credCols.put("password", password);
        return credCols;
    }

    private static void delete(Configuration config, String[] args) 
            throws IOException {
        
        Delete row = new Delete(toBytes(args[1]));
        
        // get a table and delete the row
        try 
        {            
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf("User"));
            table.delete(row);
        }    
        finally
        {
            table.close();
            connection.close();
        }
    }

    private static void show(Configuration config, String[] args) 
            throws IOException {
        
        Get row = new Get(toBytes(args[1]));

        // get a table and show a row
        try 
        {            
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf("User"));
            Result result = table.get(row);
            print(result);
        }    
        finally
        {
            table.close();
            connection.close();
        }
    }

    private static void print(Result result) {
        System.out.println("--------------------------------"); 
        System.out.println("RowId=" + Bytes.toString(result.getRow()));
        
        HashMap<String, String> credCols = SetupCredsMap("email", "password");
        
        for (String key : credCols.keySet() ) {
            String keyName = credCols.get(key);
            byte [] val = result.getValue(toBytes("creds"), toBytes(keyName));
            System.out.println("creds:" + keyName + "="+Bytes.toString(val));
        }
        
        HashMap<String, String> prefCols = SetupPrefsMap(
                "status", "date_of_birth", "security_question", "security_answer");
        
        for (String key : prefCols.keySet() ) {
            String keyName = credCols.get(key);
            byte [] val = result.getValue(toBytes("prefs"), toBytes(keyName));
            System.out.println("prefs:" + keyName + "="+Bytes.toString(val));
        }
        
//        byte [] val1 = result.getValue(toBytes("creds"), toBytes("email"));
//        System.out.println("creds:email="+Bytes.toString(val1));
    }
    
    
}
