package ds.mapreduce.talkingdata;

import org.apache.hadoop.mapreduce.Partitioner;
import ds.mapreduce.talkingdata.datatypes.PairOfStringAndLongWritable;
import org.apache.hadoop.io.Writable;

public final class AggregationPartitioner extends Partitioner<PairOfStringAndLongWritable, Writable> {

    @Override
    public int getPartition(PairOfStringAndLongWritable key, Writable value, int numberOfReducers) {

        return (key.getFirstElement().hashCode() & Integer.MAX_VALUE) % numberOfReducers;


    }

}