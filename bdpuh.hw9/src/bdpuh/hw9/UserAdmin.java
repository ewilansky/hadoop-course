/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw9;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
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
                    listall(config);                
                } else {
                    System.out.println("The Listall request takes no "
                            + "additional arguments");
                }
                
                break;
            }
            case "login": {
                if (args.length == 4) {
                    login(config, args);
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

        // add a rowRead to the table
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
        
        // get a table and add the rowRead
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
            String loginId, String password, String ipAddress) {
    
        HashMap<String, String> loginCols = new HashMap<>();

        SimpleDateFormat dateFormatter = 
                new SimpleDateFormat("yyyy/MM/dd");
        
        SimpleDateFormat timeFormatter = 
                new SimpleDateFormat("HH:mm:ss");

       
        // get the current local date and time
        LocalDateTime now = LocalDateTime.now();
        
        String date = dateFormatter.format(now);
        String time = timeFormatter.format(now);
        
        loginCols.put("ip", ipAddress);
        loginCols.put("date", date);
        loginCols.put("time", time);
        
        return loginCols;
    
    }
    
    private static void login(Configuration config, String[] args) 
            throws IOException {
        
        String password = "";
        
        HashMap<String, String> lastLoginCols = 
            SetupLastLoginMap(args[1], args[2], args[3]);
        
        // get the current row for reading (for password retrieval in this method)
        Get rowRead = new Get(toBytes(args[1]));
        
        // get the current row for writing
        Put rowWrite = new Put(toBytes(args[1]));
        
        // write the known column and values to the lastLogin column family
        for (String key : lastLoginCols.keySet()) {
            String val = lastLoginCols.get(key);
            rowWrite.addColumn(toBytes("lastLogin"), toBytes(key), toBytes(val));
        }

        // get a table and show a rowRead
        try 
        {            
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf("User"));
            byte[] passwordField = 
                    table.get(rowRead).getValue(toBytes("creds"), toBytes("password"));
            password = Bytes.toString(passwordField);
            
            // check if there is a password match
            Boolean success = args[2].equals(password);
            
            // write the success value to the column
            rowWrite.addColumn(toBytes("lastLogin"), toBytes("success"), toBytes(success.toString()));
            
            // write the row update
            table.put(rowWrite);
            
        }    
        finally
        {
            table.close();
            connection.close();
        }      
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
        
        // get a table and delete the rowRead
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

        // get a table and show a rowRead
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
        
        Map<String, List<String>> cols = new LinkedHashMap<>();
                
                
        cols.put("creds", 
                new ArrayList<>(Arrays.asList("email", "password")));
        
        cols.put("prefs", 
                new ArrayList<>(Arrays.asList(
                        "status", "date_of_birth", 
                        "security_question", "security_answer")));
        
        cols.put("lastlogin", 
                new ArrayList<>(Arrays.asList(
                        "ip", "date", "time", "success"
                )));
        
        for (Entry<String, List<String>> entry : cols.entrySet()) {
              String key = entry.getKey();
              List<String> values = entry.getValue();
              
              for(String value : values) {
                byte [] val = result.getValue(toBytes(key), toBytes(value));
                System.out.println(key + ":" + value + "=" + Bytes.toString(val)); 
              }
        }
    }   

    private static void listall(Configuration config) 
            throws IOException {
        
        Scan scan = new Scan();
   
        // get a table and can the rows
        try 
        {            
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf("User"));
            ResultScanner scanner = table.getScanner(scan);
            for (Result rowResult = scanner.next(); rowResult != null; 
                    rowResult = scanner.next()) {
                
                print(rowResult);
            }
        }    
        finally
        {
            table.close();
            connection.close();
        }
    }    
}
