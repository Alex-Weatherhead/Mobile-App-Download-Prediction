package ds.mapreduce.talkingdata;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Arrays;

public final class MyMapper extends Mapper<LongWritable, Text, TextPairWritable, Text> {

    private static final Logger LOGGER = Logger.getLogger(MyMapper.class);

    private TextPairWritable intermediateKey = new TextPairWritable();
    private Text intermediateKeyLeftElement = new Text();
    private Text intermediateKeyRightElement = new Text();
    private Text intermediateValue = new Text();

    private StringBuilder selectedFieldsStringBuilder = new StringBuilder();
    private boolean isSelectedFieldsStringBuilderEmpty; // A boolean indicator which allows us to avoid calling the length function frequently.
    private int targetIndex;
    private int [] groupByIndices;
    private int indexForGroupByIndices; // An index into the groupByIndices array.
    private int lengthOfGroupByIndices; // The length of the groupByIndices array. 

    @Override
    public void setup (Context context) {

        this.targetIndex = context.getConfiguration().getInt("targetIndex", -1);
        this.groupByIndices = context.getConfiguration().getInts("groupByIndices");
        this.lengthOfGroupByIndices = this.groupByIndices.length;

    }

    @Override
    public void map (LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {

        // Should strip the inputValue string first, because in subsequent MR jobs, the (key,value) outputting will
        // create a bunch of whitespace at the beginning; each byte of which will have to be lugged around the network.

        String [] fields = inputValue.toString().split(Constants.COMMA_DELIMETER);
        String target = fields[this.targetIndex];

        selectFields(fields);

        this.intermediateKeyLeftElement.set(this.selectedFieldsStringBuilder.toString());
        this.intermediateKey.setLeftElement(intermediateKeyLeftElement);

        this.intermediateKeyRightElement.set(Constants.FIRST);
        this.intermediateKey.setRightElement(intermediateKeyRightElement);
        this.intermediateValue.set(Constants.EMPTY);
        context.write(intermediateKey, intermediateValue);
            
        if (target.equals(Constants.ATTRIBUTED)) {
            
            this.intermediateKeyRightElement.set(Constants.SECOND);
            this.intermediateKey.setRightElement(intermediateKeyRightElement);
            this.intermediateValue.set(Constants.EMPTY);
            context.write(intermediateKey, intermediateValue);

        }

        this.intermediateKeyRightElement.set(Constants.THIRD);
        this.intermediateKey.setRightElement(intermediateKeyRightElement);
        this.intermediateValue.set(inputValue);
        context.write(intermediateKey, intermediateValue);

        

    }

    public void selectFields (String [] fields) {
        
        String field;
        this.indexForGroupByIndices = 0;
        while (this.indexForGroupByIndices < this.lengthOfGroupByIndices) {

            if (!this.isSelectedFieldsStringBuilderEmpty) {
                this.selectedFieldsStringBuilder.append(Constants.SPACE_DELIMETER);
            }
            else {
                this.isSelectedFieldsStringBuilderEmpty = false;
            }

            field = fields[this.groupByIndices[this.indexForGroupByIndices]];
            this.selectedFieldsStringBuilder.append(field);

            this.indexForGroupByIndices += 1;

        }

    }

    public void unselectFields () {

        this.selectedFieldsStringBuilder.delete(0, this.selectedFieldsStringBuilder.length());
        this.isSelectedFieldsStringBuilderEmpty = false;

    }


} 