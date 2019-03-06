package ds.mapreduce.talkingdata.datatypes;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

/**
 * While a parent class based on generics would improve design drastically, the use of primitive types 
 * is imperative to the performance of algorithms that make heavy use this data structure.
 * 
 * Since the hashCode() function is not overriden, it should not be used for partitioning purposes. 
 */
public class PairOfStringAndLongWritable implements WritableComparable<PairOfStringAndLongWritable> {

    private String firstElement;
    private long secondElement;

    public PairOfStringAndLongWritable () {}

    public PairOfStringAndLongWritable (String firstElement, long secondElement) {

        this.firstElement = firstElement;
        this.secondElement = secondElement;

    }

    /**
     * Set the values of the pair's String and long element.
     * 
     * @param firstElement the value with which to set the pair's String element.
     * @param secondElement the value with which to set the pair's long element.
     */
    public void setElements (String firstElement, long secondElement) {

        this.firstElement = firstElement;
        this.secondElement = secondElement;
    
    }

    /**
     * Set the value of the pair's String element.
     * 
     * @param firstElement the value with which to set the pair's String element.
     */
    public void setFirstElement (String firstElement) {

        this.firstElement = firstElement;

    }

    /**
     * Set the value of the pair's long element.
     *
     * @param secondElement the value with which to set the pair's long.
     */
    public void setSecondElement (long secondElement) {

        this.secondElement = secondElement;

    }

    /**
     * Get the value of the pair's String element.
     *
     * @return the value of the pair's String element.
     */
    public String getFirstElement () {

        return this.firstElement;

    }

    /**
     * Get the value of the pair's long element.
     *
     * @return the value of the pair's long element.
     */
    public long getSecondElement () {

        return this.secondElement;

    }

    /**
     * Serialize the object by converting the values of the object's String and long elements to bytes 
     * and writing them to a binary output stream.
     *
     * @param dataOutputStream the binary output stream in which to write the bytes of object's String and long elements.
     */
    @Override
    public void write (DataOutput dataOutputStream) throws IOException {

        dataOutputStream.writeUTF(this.firstElement);
        dataOutputStream.writeLong(this.secondElement);

    }

    /**
     * Deserialize the object by reading bytes from a binary input stream and converting
     * them to values of the object's String and long values.
     *
     * @param dataInputStream the binary input stream from which to read the bytes of the object's long and int elements.
     */
    @Override
    public void readFields (DataInput dataInputStream) throws IOException {

        this.firstElement = dataInputStream.readUTF();
        this.secondElement = dataInputStream.readLong();
        
    }

    /**
     * Compares this pair to another by looking first at each pairs' String element, and then at each pair's long element.
     *
     * Examples:
     *    
     *       ("apple", 8)  ?  ("banana", 8) = -1
     *      ("banana", 3)  ?  ("banana", 4) = -1
     *       ("apple", 1)  ?  ("banana", 2) = -1
     *      ("orange", 9)  ?  ("orange", 9) =  0
     *      ("banana", 2)  ?  ("apple", 2)  = +1
     *      ("banana", 6)  ?  ("banana", 5) = +1
     *      ("banana", 3)  ?  ("apple", 2)  = +1
     *
     * @return -1, 0, or +1 depending on whether this object is less than, equal to, or greater than the other.
     */
    @Override 
    public int compareTo (PairOfStringAndLongWritable that) {

        int comparison;
        
        comparison = this.firstElement.compareTo(that.firstElement);

        if (comparison == 0) {

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

        return comparison;

    }

}