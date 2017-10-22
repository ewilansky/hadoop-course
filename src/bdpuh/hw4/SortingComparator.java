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
public class SortingComparator extends WritableComparator {
    
    protected SortingComparator() {
        super(CompositeKeyWritable.class, true);
    }

@Override
public int compare(WritableComparable inputKey1, WritableComparable inputKey2) {
    // Sort on all attributes of composite key
    CompositeKeyWritable key1 = (CompositeKeyWritable) inputKey1;
    CompositeKeyWritable key2 = (CompositeKeyWritable) inputKey2;

    int cmpResult = key1.getjoinKey().compareTo(key2.getjoinKey());
    if (cmpResult == 0)// same joinKey
    {
        return Double.compare(key1.getsourceIndex(), key2.getsourceIndex());
    }
    return cmpResult;
    }
}