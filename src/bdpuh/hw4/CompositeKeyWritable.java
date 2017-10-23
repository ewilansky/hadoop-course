/*
 * EN.605.788.81.FA17 Big Data Processing Using Hadoop
 * Student/Author: Ethan Wilansky
 * Adapted from: CompositeKeyWritableRSJ by Anagha Khanolkar
 */
package bdpuh.hw4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 * @author ethanw
 */
public class CompositeKeyWritable 
        implements Writable, WritableComparable<CompositeKeyWritable> {
    
    // Data members
    private String joinKey; // MovieID
    private int sourceIndex; // 1=Movie data 2=Ratings data; 

    public CompositeKeyWritable() {
        System.out.println("In CompositeKeyWritable");    
    }

    public CompositeKeyWritable(String joinKey, int sourceIndex) {
            this.joinKey = joinKey;
            this.sourceIndex = sourceIndex;
            
            System.out.println("In CompositeKeyWritable. joinKey: " + joinKey + " sourceIndex: " + sourceIndex);    
    
    }

    @Override
    public String toString() {
        return (new StringBuilder().append(joinKey).append("\t")
                        .append(sourceIndex)).toString();
    }

    public void readFields(DataInput dataInput) throws IOException {
        System.out.println("In Composite readFields: joinKey:" + joinKey + " sourceIndex: " + sourceIndex);
        joinKey = WritableUtils.readString(dataInput);
        sourceIndex = WritableUtils.readVInt(dataInput);
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, joinKey);
        WritableUtils.writeVInt(dataOutput, sourceIndex);
    }

    public int compareTo(CompositeKeyWritable objKeyPair) {
        int result = joinKey.compareTo(objKeyPair.joinKey);
        if (0 == result) {
                result = Double.compare(sourceIndex, objKeyPair.sourceIndex);
        }
        return result;
    }

    public String getjoinKey() {
        return joinKey;
    }

    public void setjoinKey(String joinKey) {
        this.joinKey = joinKey;
    }

    public int getsourceIndex() {
            return sourceIndex;
    }

    public void setsourceIndex(int sourceIndex) {
            this.sourceIndex = sourceIndex;
    }  
}
