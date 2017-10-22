/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author ethanw
 */
public class MovieRatingsPartitioner 
        extends Partitioner<Integer, IntWritable> {
    
    int i = 0;
    IntWritable count = new IntWritable();
    CompositeKeyWritable compositeKey = new CompositeKeyWritable();    

    @Override
    public int getPartition(Integer key, IntWritable value,
               int numReducers) {
        
        // Assignment states 2 reducers.
        // There are 1682 movies rated so movie ids run from 1 - 1682           
        Integer intKey = Integer.parseInt(compositeKey.getjoinKey());
        System.out.printf("In partitioner, intKey: {0}", intKey);
        
        Integer partition = intKey / 2;
        
        if (intKey <= partition) {
            return 0;
        } else {
            return 1;
        } 
    }
}
