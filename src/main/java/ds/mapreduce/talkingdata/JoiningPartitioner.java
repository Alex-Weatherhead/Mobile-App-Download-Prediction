package ds.mapreduce.talkingdata;

import org.apache.hadoop.mapreduce.Partitioner;
import ds.mapreduce.talkingdata.datatypes.PairOfLongAndIntWritable;
import org.apache.hadoop.io.Writable;

public final class JoiningPartitioner extends Partitioner<PairOfLongAndIntWritable, Writable> {

    @Override
    public int getPartition(PairOfLongAndIntWritable key, Writable value, int numberOfReducers) {

        return ((int) key.getFirstElement() & Integer.MAX_VALUE) % numberOfReducers;

    }

}