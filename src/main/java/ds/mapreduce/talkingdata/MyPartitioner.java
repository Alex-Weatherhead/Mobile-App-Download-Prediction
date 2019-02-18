package ds.mapreduce.talkingdata;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public final class MyPartitioner extends Partitioner<TextPairWritable, Text> {

    @Override
    public int getPartition(TextPairWritable key, Text value, int numberOfReducers) {
            
        return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numberOfReducers;

    }

}