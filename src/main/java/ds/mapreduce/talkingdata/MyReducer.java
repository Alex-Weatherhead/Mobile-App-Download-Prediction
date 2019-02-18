package ds.mapreduce.talkingdata;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.util.Iterator;
import java.io.IOException;

public final class MyReducer extends Reducer<TextPairWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    private double numberOfRecordsForIntermediateKeyLeftElement = 0;
    private double numberOfAttributionsForIntermediateKeyLeftElement = 0;

    @Override
    public void reduce (TextPairWritable intermediateKey, Iterable<Text> intermediateValues, Context context) throws IOException, InterruptedException {
        
        Text intermediateKeyLeftElement = intermediateKey.getLeftElement();
        Text intermediateKeyRightElement = intermediateKey.getRightElement();

        Iterator<Text> intermediateValueIterator = intermediateValues.iterator();

        if (intermediateKeyRightElement.equals(Constants.THIRD)) {
            
            outputKey.set(Constants.EMPTY); // The entire record is contained in the value, so the key is not important.

            Text intermediateValue = null; 

            // Concatenate the average number of attributions per record with the given intermediateKeyLeftElement to each record with that key.
            while (intermediateValueIterator.hasNext()) {
                
                intermediateValue = intermediateValueIterator.next(); 

                outputValue.set(intermediateValue.toString() + Constants.COMMA_DELIMETER + numberOfAttributionsForIntermediateKeyLeftElement/numberOfRecordsForIntermediateKeyLeftElement);

                context.write(outputKey, outputValue);

            }

        }
        else if (intermediateKeyRightElement.equals(Constants.SECOND)) {

            this.numberOfAttributionsForIntermediateKeyLeftElement = 0;

            // Retrieve the number of attributions for the given intermediateKeyLeftElement.
            while (intermediateValueIterator.hasNext()) {
                this.numberOfAttributionsForIntermediateKeyLeftElement += 1;
                intermediateValueIterator.next();
            }

        }
        else if (intermediateKeyRightElement.equals(Constants.FIRST)) {

            this.numberOfRecordsForIntermediateKeyLeftElement = 0;

            // Retreive the number of records for the given intermediateKeyLeftElement.
            while (intermediateValueIterator.hasNext()) {
                this.numberOfRecordsForIntermediateKeyLeftElement += 1;
                intermediateValueIterator.next();
            }

        }

    }

} 