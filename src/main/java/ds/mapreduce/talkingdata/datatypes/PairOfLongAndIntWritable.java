package ds.mapreduce.talkingdata.datatypes;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

/**
 * Note: While a parent class based on generics would improve design drastically, the use of 
 * primitive types is imperative to the performance of algorithms that make heavy use this data
 * structure.
 * 
 * Note: since the hashCode() function is not overriden, it should not be used for partitioning
 * purposes, as the values returned may not be consistent across jvm instances.
 */
public class PairOfLongAndIntWritable implements WritableComparable<PairOfLongAndIntWritable> {

    private long firstElement;
    private int secondElement;

    public PairOfLongAndIntWritable () {}

    public PairOfLongAndIntWritable (long firstElement, int secondElement) {

        this.firstElement = firstElement;
        this.secondElement = secondElement;

    }

    /**
     * Set the values of the pair's long and int elements.
     * 
     * @param firstElement the value with which to set the pair's long elmenet.
     * @param secondElement the value with which to set the pair's int element.
     */
    public void setElements (long firstElement, int secondElement) {

        this.firstElement = firstElement;
        this.secondElement = secondElement;
    
    }

    /**
     * Set the value of the pair's long element.
     * 
     * @param firstElement the value with which to set the pair's long element.
     */
    public void setFirstElement (long firstElement) {

        this.firstElement = firstElement;

    }

    /**
     * Set the value of the pair's int element.
     *
     * @param secondElement the value with which to set the pair's int element.
     */
    public void setSecondElement (int secondElement) {

        this.secondElement = secondElement;

    }

    /**
     * Get the value of the pair's long element.
     *
     * @return the value of the pair's long element.
     */
    public long getFirstElement () {

        return this.firstElement;

    }

    /**
     * Get the value of the pair's int elemnet.
     *
     * @return the value of the pair's int element.
     */
    public int getSecondElement () {

        return this.secondElement;

    }

    /**
     * Serialize the object by converting the values of the object's long and int elements to bytes
     * and writing them to a binary output stream.
     *
     * @param dataOutputStream the binary output stream in which to write the bytes of the object's long and int elements.
     */
    @Override
    public void write (DataOutput dataOutputStream) throws IOException {

        dataOutputStream.writeLong(this.firstElement);
        dataOutputStream.writeInt(this.secondElement);

    }

    /**
     * Deserialize the object by reading bytes from a binary input stream and converting
     * them to values of the object's long and int elements.
     *
     * @param dataInputStream the binary input stream from which to read the bytes of the object's long and int elements.
     */
    @Override
    public void readFields (DataInput dataInputStream) throws IOException {

        this.firstElement = dataInputStream.readLong();
        this.secondElement = dataInputStream.readInt();

    }

    /**
     * Compares this pair to another by looking first at each pairs' long element, and then at each pair's int element.
     *
     * Examples:
     *      
     *       (6, 2) ? (7, 2)  = -1
     *       (7, 3) ? (7, 4)  = -1
     *       (6, 3) ? (7, 4)  = -1
     *       (8, 2) ? (8, 2)  = 0
     *      (11, 1) ? (10, 1) = +1
     *      (10, 8) ? (10, 7) = +1
     *      (13, 5) ? (12, 4) = +1
     *
     * @return -1, 0, or +1 depending on whether this object is less than, equal to, or greater than the other.
     */
    @Override 
    public int compareTo (PairOfLongAndIntWritable that) {

        int comparison;

        if (this.firstElement < that.firstElement) {

            comparison = -1;
            
        }
        else if (this.firstElement == that.firstElement) {

            if (this.secondElement < that.secondElement) {

                comparison = -1;

            }
            else if (this.secondElement == that.secondElement) {

                comparison = 0;
            
            }
            else {

                comparison = +1;

            }

        }
        else {

            comparison = +1;

        }

        return comparison;
    
    }

}