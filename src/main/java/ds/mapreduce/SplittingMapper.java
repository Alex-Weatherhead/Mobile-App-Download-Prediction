package ds.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import ds.mapreduce.talkingdata.exceptions.MissingConfigurationPropertyException;
import ds.mapreduce.talkingdata.exceptions.InvalidConfigurationPropertyValueException;
import java.util.Arrays;

public class SplittingMapper extends Mapper <LongWritable, Text, NullWritable, Text> {

    private static final String COMMA_DELIMETER = ",";
    private static final String MISSING = " ";

    private NullWritable intermediateKey = NullWritable.get();
    private Text intermediateValue = new Text();

    private int splitIndex;
    private String delimeter;
    private int limit;
    private boolean keepLast;

    protected void setup (Context context) throws IOException, InterruptedException {

        try {

            this.splitIndex = context.getConfiguration().getInts("splitIndex")[0]; // Using getInts() over getInt() allows for an easier distinction between the two possible exceptions, as we do not have to account for a default value.

        }
        catch (ArrayIndexOutOfBoundsException e) {

            throw new MissingConfigurationPropertyException("No index provided for the attribute to be split.");

        }
        
        if (this.splitIndex < 0) {

            throw new InvalidConfigurationPropertyValueException("A negative index was provided for the attribute to be split.");

        }

        try {

            this.delimeter = context.getConfiguration().getStrings("delimeter")[0]; 

        }
        catch (ArrayIndexOutOfBoundsException e) {

            throw new MissingConfigurationPropertyException("No delimeter provided for the split."); 

        }

        try {

            this.limit = context.getConfiguration().getInts("limit")[0]; // Using getInts() over getInt() allows for an easier distinction between the two possible exceptions, as we do not have to account for a default value.

        }
        catch (ArrayIndexOutOfBoundsException e) {

            throw new MissingConfigurationPropertyException("No limit provided for the split.");

        }

        if (this.limit < 0) {

            throw new InvalidConfigurationPropertyValueException("A negative limit was provided for the split.");

        }

        this.keepLast = context.getConfiguration().getBoolean("keepLast", true);

    }

    public void map (LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {

        String [] attributes = inputValue.toString().split(COMMA_DELIMETER);

        if (!attributes[this.splitIndex].equals(MISSING)) {

            String [] splits = attributes[this.splitIndex].split(this.delimeter, this.limit);

            if (!this.keepLast) {

                splits = Arrays.copyOfRange(splits, 0, splits.length - 1);

            }

            attributes[this.splitIndex] = String.join(COMMA_DELIMETER, splits);

            int i = splits.length;
            while (i < this.limit) {

                attributes[this.splitIndex] += COMMA_DELIMETER; // Fill in the 'missing' splits with blanks so that each record still has the same number of attributes.

                i ++;

            }

        }

        this.intermediateValue.set(String.join(COMMA_DELIMETER, attributes));

        context.write(this.intermediateKey, this.intermediateValue);

    }


}