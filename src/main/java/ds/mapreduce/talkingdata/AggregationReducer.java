package ds.mapreduce.talkingdata;

import org.apache.hadoop.mapreduce.Reducer;
import ds.mapreduce.talkingdata.datatypes.PairOfStringAndLongWritable;
import org.apache.hadoop.io.BooleanWritable;
import ds.mapreduce.talkingdata.datatypes.PairOfLongAndIntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import ds.mapreduce.talkingdata.exceptions.MissingConfigurationPropertyException;
import ds.mapreduce.talkingdata.exceptions.InvalidConfigurationPropertyValueException;
import org.apache.hadoop.io.WritableUtils;
import java.util.Iterator;

public class AggregationReducer extends Reducer <PairOfStringAndLongWritable, BooleanWritable, PairOfLongAndIntWritable, DoubleWritable> {

    private static final Logger LOGGER = LogManager.getLogger(AggregationReducer.class.getName());

    private ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    private DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    private String intermediateKeyStringElement;
    private long intermediateKeyLongElement;
    private long previousIntermediateKeyLongElement;

    private PairOfLongAndIntWritable outputKey = new PairOfLongAndIntWritable();
    private DoubleWritable outputValue = new DoubleWritable();

    private long numberOfPositiveTargetValues;
    private long numberOfTargetValues;
    
    private int postJoiningIndex;

    /**
     * Loads in the post-joining index of the aggregate from the job's configuration. 
     * 
     * @exception MissingConfigurationPropertyException if no post-joining index is provided for the aggregate.
     * @exception InvalidConfigurationPropertyValueException if the post-joining index is provided for the aggregate.
     * @param context the context passed to this reducer.
     */
    @Override
    protected void setup (Context context) throws IOException, InterruptedException {

        try {

            this.postJoiningIndex = context.getConfiguration().getInts("postJoiningIndex")[0];

        }
        catch (ArrayIndexOutOfBoundsException e) {

            throw new MissingConfigurationPropertyException("No post joining index provided for the aggregate.");

        }

        if (this.postJoiningIndex < 0) {

            throw new InvalidConfigurationPropertyValueException("A negative post joining index was provided for the aggregate.");

        }

        this.outputKey.setSecondElement(this.postJoiningIndex);

    }

    /**
     * Keeps track (globally) of the proportion of records with the same set of aggregation attributes which also have a 
     * target value of true. Once all such records are seen, each is emmitted as a (key, value) pair of the form: 
     * ((index, post-joining index), number of true target values seen / number of target values seen).
     * 
     * In order to calculate the aforementioned proportion, all the indices sharing the same set of aggregation attributes
     * have to be buffered in memory. Such buffering can be very taxing. Therefore, secondary sorting is used to ensure 
     * that the indices arrive in sorted order so that gap compression can be used to temporarily store the indices. Once 
     * all the records with the same set of aggregation attributes have been seen, the indices are recovered and the 
     * appropriate (key, value) pairs are emitted.
     * 
     * @param intermediateKey a composite key consisting of the aggregation attributes and the index of a record containing that value.
     * @param intermediateValues an iterable consisting of a single boolean: true if the record's target value is true, and false if the record's target value is false.
     * @param context the context passed to this reducer.
     */
    @Override
    protected void reduce (PairOfStringAndLongWritable intermediateKey, Iterable <BooleanWritable> intermediateValues, Context context) throws IOException, InterruptedException {

        if (this.intermediateKeyStringElement == null) {

            this.intermediateKeyStringElement = intermediateKey.getFirstElement();
            
        }
        else if (!this.intermediateKeyStringElement.equals(intermediateKey.getFirstElement())) {

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(this.byteArrayOutputStream.toByteArray());
            DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);

            this.outputValue.set(((double) this.numberOfPositiveTargetValues) / this.numberOfTargetValues);
            
            this.previousIntermediateKeyLongElement = 0; // Reset to 0 so that we can perform gap decompression in the following loop.

            int i = 0;
            while (i < this.numberOfTargetValues) {

                this.intermediateKeyLongElement = WritableUtils.readVLong(dataInputStream);
                this.outputKey.setFirstElement(this.intermediateKeyLongElement + this.previousIntermediateKeyLongElement);
                this.previousIntermediateKeyLongElement += this.intermediateKeyLongElement;

                context.write(this.outputKey, this.outputValue);

                i ++;

            }

            this.previousIntermediateKeyLongElement = 0; // Reset to 0 so that we can perform gap compression outside of this if-else block. 
            this.numberOfPositiveTargetValues = 0;
            this.numberOfTargetValues = 0;
            this.byteArrayOutputStream.reset();

            this.intermediateKeyStringElement = intermediateKey.getFirstElement();

        }

        this.intermediateKeyLongElement = intermediateKey.getSecondElement();
        WritableUtils.writeVLong(this.dataOutputStream, this.intermediateKeyLongElement - this.previousIntermediateKeyLongElement); 
        this.previousIntermediateKeyLongElement = this.intermediateKeyLongElement;

        this.numberOfTargetValues ++;

        Iterator <BooleanWritable> intermediateValuesIterator = intermediateValues.iterator();
        BooleanWritable intermediateValue = intermediateValuesIterator.next();
        this.numberOfPositiveTargetValues += intermediateValue.get() ? 1 : 0;

    }   

}
