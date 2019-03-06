package ds.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.io.IOException;
import ds.mapreduce.talkingdata.exceptions.MissingConfigurationPropertyException;
import ds.mapreduce.talkingdata.exceptions.InvalidConfigurationPropertyValueException;
import java.util.Arrays;

public class DiscretizationMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private static final Logger LOGGER = LogManager.getLogger(DiscretizationMapper.class.getName());

    private static final String COMMA_DELIMETER = ",";
    private static final String MISSING = "";

    private NullWritable intermediateKey = NullWritable.get();
    private Text intermediateValue = new Text();

    private int discretizeIndex;
    private String [] bins;

    @Override
    protected void setup (Context context) throws IOException, InterruptedException {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("setup() - start");
        }

        try {

            this.discretizeIndex = context.getConfiguration().getInts("discretizeIndex")[0]; // Using getInts() over getInt() allows for an easier distinction between the two possible exceptions, as we do not have to account for a default value.

        }
        catch (ArrayIndexOutOfBoundsException e) {

            throw new MissingConfigurationPropertyException("No index provided for the attribute to be discretized.");

        }
        
        if (this.discretizeIndex < 0) {

            throw new InvalidConfigurationPropertyValueException("A negative index was provided for the attribute to be discretized.");

        }

        this.bins = context.getConfiguration().getStrings("bins");
        
        int lengthOfBins= this.bins.length;
        if (lengthOfBins == 0) {

            throw new MissingConfigurationPropertyException("No bins were provided.");

        }
        else {

            int i = 0;
            while (i < lengthOfBins) {

                if (this.bins[i] == null) {

                    throw new InvalidConfigurationPropertyValueException("A null value was provided in the bins.");

                }

                i ++;

            }

        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("discretizeIndex: " + this.discretizeIndex);
            LOGGER.debug("bins: " + Arrays.toString(this.bins));
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("setup() - stop");
        }

    }

    @Override
    protected void map (LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("map() - start");
        }

        String [] attributes = inputValue.toString().split(COMMA_DELIMETER);

        if (!attributes[this.discretizeIndex].equals(MISSING)) {

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Value prior to discretization: " + attributes[this.discretizeIndex]);
            }

            boolean isBinned = false;

            int lengthOfBins = this.bins.length;
            int i = 1;
            while (i < lengthOfBins && !isBinned) {

                if (attributes[this.discretizeIndex].compareTo(this.bins[i]) == -1) {
    
                    attributes[this.discretizeIndex] = this.bins[i - 1];
                    isBinned = true;
        
                }

                i ++;
    
            }

            if (!isBinned) {

                attributes[this.discretizeIndex] = this.bins[lengthOfBins - 1];

            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Value after discretization: " + attributes[this.discretizeIndex]);
            }

        }

        this.intermediateValue.set(String.join(COMMA_DELIMETER, attributes));

        context.write(intermediateKey, intermediateValue);

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("map() - stop");
        }

    }


}