/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 */
package bdpuh.hw4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author ethanw
 */
public class GroupingComparator extends WritableComparator {
    protected GroupingComparator() {
        super(CompositeKeyWritable.class, true);
    }
    
    @Override
    public int compare(WritableComparable inputKey1, WritableComparable inputKey2) {
        // The grouping comparator is the joinKey (Movie Id)
        CompositeKeyWritable key1 = (CompositeKeyWritable) inputKey1;
        CompositeKeyWritable key2 = (CompositeKeyWritable) inputKey2;
        return key1.getjoinKey().compareTo(key2.getjoinKey());
    } 
}
