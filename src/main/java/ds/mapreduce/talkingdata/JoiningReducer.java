package ds.mapreduce.talkingdata;

import org.apache.hadoop.mapreduce.Reducer;
import ds.mapreduce.talkingdata.datatypes.PairOfLongAndIntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.Iterator;

public class JoiningReducer extends Reducer <PairOfLongAndIntWritable, DoubleWritable, NullWritable, Text> {

    private static final long UNINITIALIZED_INTERMEDIATE_KEY_LONG_VALUE = -1;
    public static final String COMMA_DELIMETER = ",";
    public static final int LENGTH_OF_COMMA_DELIMETER = 1;

    private long intermediateKeyLongElement = UNINITIALIZED_INTERMEDIATE_KEY_LONG_VALUE;

    private final NullWritable outputKey = NullWritable.get();
    private Text outputValue = new Text();

    private StringBuilder outputValueStringBuilder = new StringBuilder();
    private int lengthOfOutputValueString;

    /**
     * Concatenates aggregates with a comma delimeter in the order of their post-joining indices. 
     * Once all aggregates associated with an index have been seen, emits a (key, value) 
     * pair of the form: (NULL, concatenated aggregates for that index).
     * 
     * Secondary sorting is used to ensure that the aggregates are concatenated in the 
     * proper order based on their aggregate indices.
     * 
     * @param intermediateKey a composite key consisting of the index of a record and the post-joining index of the aggregate.
     * @param intermediateValue an aggregate computed on the attributes of the record given by the index.
     * @param context the context passed to this reducer.
     */
    @Override 
    protected void reduce (PairOfLongAndIntWritable intermediateKey, Iterable <DoubleWritable> intermediateValues, Context context) throws IOException, InterruptedException {

        if (this.intermediateKeyLongElement == UNINITIALIZED_INTERMEDIATE_KEY_LONG_VALUE) {

            this.intermediateKeyLongElement = intermediateKey.getFirstElement();
            
        }
        else if (this.intermediateKeyLongElement != intermediateKey.getFirstElement()) {

            this.outputValue.set(this.outputValueStringBuilder.substring(0, this.lengthOfOutputValueString));
            context.write(this.outputKey, this.outputValue); 

            this.lengthOfOutputValueString = 0;

        }

        if (this.lengthOfOutputValueString > 0) {

            this.outputValueStringBuilder.replace(this.lengthOfOutputValueString, this.lengthOfOutputValueString + LENGTH_OF_COMMA_DELIMETER, COMMA_DELIMETER);
            this.lengthOfOutputValueString += LENGTH_OF_COMMA_DELIMETER;

        }

        Iterator <DoubleWritable> intermediateValuesIterator = intermediateValues.iterator();
        DoubleWritable intermediateValue = intermediateValuesIterator.next();
        String intermediateValueString = intermediateValue.toString();
        this.outputValueStringBuilder.replace(this.lengthOfOutputValueString, this.lengthOfOutputValueString + intermediateValueString.length(), intermediateValueString);
        this.lengthOfOutputValueString += intermediateValueString.length();

    }

}