package ds.mapreduce.talkingdata;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import ds.mapreduce.talkingdata.datatypes.PairOfStringAndLongWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.io.IOException;
import ds.mapreduce.talkingdata.exceptions.MissingConfigurationPropertyException;
import ds.mapreduce.talkingdata.exceptions.InvalidConfigurationPropertyValueException;
import java.util.Arrays;

public class AggregationMapper extends Mapper <LongWritable, Text, PairOfStringAndLongWritable, BooleanWritable> {

    private static final Logger LOGGER = LogManager.getLogger(AggregationMapper.class.getName());

    private static final String COMMA_DELIMETER = ",";
    private static final int LENGTH_OF_COMMA_DELIMETER = 1;
    private static final String TARGET_VALUE_TRUE = "1";
    
    private PairOfStringAndLongWritable outputKey = new PairOfStringAndLongWritable();
    private BooleanWritable outputValue = new BooleanWritable();

    private int targetIndex;
    private int [] aggregationIndices;

    private StringBuilder intermediateKeyStringBuilder = new StringBuilder(); // Helps alleviate excessive object creation while concatenating record attributes.
    private int lengthOfIntermediateKeyString; // Keeps track of how long the String in the StringBuilder actually is in the current mapper.

    /**
     * Loads in the index of the target attribute and the indices of the aggregation attributes
     * from the job's configuration. 
     * 
     * @exception MissingConfigurationPropertyException if no indices are provided for either the target or aggregation attributes.
     * @exception InvalidConfigurationPropertyValueException if any of the indices provided for either the target or the aggregation attributes are negative.
     * @param context the context passed to this mapper.
     */
    @Override
    protected void setup (Context context) throws IOException, InterruptedException {
        
        try {

            this.targetIndex = context.getConfiguration().getInts("targetIndex")[0]; // Using getInts() over getInt() allows for an easier distinction between the two possible exceptions, as we do not have to account for a default value.

        }
        catch (ArrayIndexOutOfBoundsException e) {

            throw new MissingConfigurationPropertyException("No index provided for the target attribute.");

        }
        
        if (this.targetIndex < 0) {

            throw new InvalidConfigurationPropertyValueException("A negative index was provided for the target attribute.");

        }
        
        this.aggregationIndices = context.getConfiguration().getInts("aggregationIndices");
        int lengthOfAggregationIndices = this.aggregationIndices.length;

        if (lengthOfAggregationIndices == 0) {

            throw new MissingConfigurationPropertyException("No indices provided for the aggregation attributes.");

        }
        else {

            int i = 0;
            while (i < lengthOfAggregationIndices) {

                if (this.aggregationIndices[i] < 0) {

                    throw new InvalidConfigurationPropertyValueException("A negative index was provided for one or more of the aggregation attributes.");

                }

                i ++;

            }

        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("targetIndex: " + this.targetIndex);
            LOGGER.debug("aggregationIndices: " + Arrays.toString(this.aggregationIndices));
        }

    }

    /**
     * Extracts all of the attributes in the record on which to aggregate (when there is more than one, 
     * these attributes are concatenated with a comma delimeter), as well as its binary target attribute,
     * and emits a (key, value) pair of the form: ((aggregation attributes, index), target attribute).
     * 
     * @param inputKey the index of the record.
     * @param inputValue the list of attributes for the record.
     * @param context the context passed to this mapper.
     */
    @Override
    protected void map (LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {

        String [] attributes = inputValue.toString().split(COMMA_DELIMETER);
    
        this.lengthOfIntermediateKeyString = 0;

        int i = 0;
        int lengthOfAggregationIndices = this.aggregationIndices.length;
        while (i < lengthOfAggregationIndices) {

            if (this.lengthOfIntermediateKeyString > 0) {

                this.intermediateKeyStringBuilder.replace(this.lengthOfIntermediateKeyString, this.lengthOfIntermediateKeyString + LENGTH_OF_COMMA_DELIMETER, COMMA_DELIMETER);
                this.lengthOfIntermediateKeyString += LENGTH_OF_COMMA_DELIMETER;

            }

            String aggregationAttribute = attributes[this.aggregationIndices[i]];
            int lengthOfAggregationAttribute = aggregationAttribute.length();

            this.intermediateKeyStringBuilder.replace(this.lengthOfIntermediateKeyString, this.lengthOfIntermediateKeyString + lengthOfAggregationAttribute, aggregationAttribute);
            this.lengthOfIntermediateKeyString += lengthOfAggregationAttribute;

            i ++;

        }

        String targetAttribute = attributes[this.targetIndex];

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Values of aggregation attributes " + this.intermediateKeyStringBuilder.substring(0, this.lengthOfIntermediateKeyString));
            LOGGER.debug("Value of target attribute " + targetAttribute);
        }

        this.outputKey.setFirstElement(this.intermediateKeyStringBuilder.substring(0, this.lengthOfIntermediateKeyString));
        this.outputKey.setSecondElement(inputKey.get());
        this.outputValue.set(targetAttribute.equals(TARGET_VALUE_TRUE));

        context.write(this.outputKey, this.outputValue);

    }

}