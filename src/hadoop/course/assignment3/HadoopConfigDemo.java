/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoop.course.assignment3;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;


public class HadoopConfigDemo {
    static {
        Configuration.addDefaultResource("hadoop/config/demo/my-app-conf.xml");
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        // dumps entire Hadoopp configuration as single string of key value pairs
        Configuration.dumpConfiguration(conf, new PrintWriter(System.out));
        
        for(Map.Entry<String, String> entry: conf) {
            System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
       }
        
       System.out.println("fs.default.name is: " + conf.get("fs.default.name")); 
       System.out.println("fs.defaultFS is " + conf.get("fs.defaultFS"));
    }
    
}
