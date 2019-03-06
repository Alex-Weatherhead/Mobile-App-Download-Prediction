package ds.mapreduce.talkingdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import ds.mapreduce.talkingdata.datatypes.PairOfStringAndLongWritable;
import org.apache.hadoop.io.BooleanWritable;
import ds.mapreduce.talkingdata.datatypes.PairOfLongAndIntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import java.util.List;

public class AggregationTool extends Configured implements Tool {

    private static final Logger LOGGER = LogManager.getLogger(AggregationTool.class.getName());

    private static final class CmdLineArgs {

        @Option(name="-inputPathname", required=true, usage="The pathname the desired input file or directory.")
        String inputPathname;
    
        @Option(name="-outputPathname", required=true, usage="The pathname of the desired output directory.")
        String outputPathname;
    
        @Option(name = "-overwriteOutputPath", required=false, usage = "Whether or not to overwrite the contents in the output directory if any exist.")
        boolean overwriteOutputPath = false;

        @Option(name = "-numberOfReducers", usage="The number of reduce tasks to use.")
        int numberOfReducers = 1;
        
        @Option(name="-aggregationIndices", required=true, usage="The indices of the columns on which to group by.", handler=MultiIntOptionHandler.class)
        List<Integer> aggregationIndices;
        
        @Option(name="-targetIndex", required=true, usage="The index of the column containing the target.")
        int targetIndex;

        @Option(name="-postJoiningIndex", required=true, usage="The index of the column in which to put the aggregate in the join phase.")
        int postJoiningIndex;

    }

    private AggregationTool () {}

    public static void main(String args []) throws Exception {

        ToolRunner.run(new AggregationTool(), args);

    }

    @Override       
    public int run(String args []) throws Exception {

        // Parse the various command-line arguments.

        CmdLineArgs cmdLineArgs = new CmdLineArgs();
        CmdLineParser cmdLineParser = new CmdLineParser(cmdLineArgs);
        
        try {

            cmdLineParser.parseArgument(args);

        }
        catch (CmdLineException e) {

            LOGGER.error(e.getMessage());
            cmdLineParser.printUsage(System.err);
            System.exit(-1);

        }

        LOGGER.info("-inputPathname: " + cmdLineArgs.inputPathname);
        LOGGER.info("-outputPathname: " + cmdLineArgs.outputPathname);
        LOGGER.info("-overwriteOutputPath: " + cmdLineArgs.overwriteOutputPath);
        LOGGER.info("-numberOfReducers: " + cmdLineArgs.numberOfReducers);
        LOGGER.info("-aggregationIndices: " + cmdLineArgs.aggregationIndices.toString());
        LOGGER.info("-targetIndex: " + cmdLineArgs.targetIndex);
        LOGGER.info("-postJoiningIndex" + cmdLineArgs.postJoiningIndex);

        // Create the configuration.

        Configuration conf = getConf();

        // Create the job.

        String jobName = AggregationTool.class.getSimpleName();

        Job job = Job.getInstance(conf);
        job.setJarByClass(AggregationTool.class);
        job.setJobName(jobName); 

        // Input path/formatting details.

        Path inputPath = new Path(cmdLineArgs.inputPathname); 
        FileInputFormat.setInputPaths(job, inputPath); 
        job.setInputFormatClass(TextInputFormat.class);

        // Output path/formatting details.

        Path outputPath = new Path(cmdLineArgs.outputPathname);
        FileOutputFormat.setOutputPath(job, outputPath); 
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        if (cmdLineArgs.overwriteOutputPath) { 

            FileSystem.get(conf).delete(outputPath, true); // Recursively delete the contents currently at the outputPath.
        
            LOGGER.info("Overwriting contents at outputPath " + outputPath.toString());

        }
        
        // Specify a Mapper, Partitioner, and Reducer.

        job.setMapperClass(AggregationMapper.class);
        job.setPartitionerClass(AggregationPartitioner.class);
        job.setReducerClass(AggregationReducer.class);

        // Specify the (key, value) intermediate/output types for the Mapper/Reducer.

        job.setMapOutputKeyClass(PairOfStringAndLongWritable.class);
        job.setMapOutputValueClass(BooleanWritable.class);
        job.setOutputKeyClass(PairOfLongAndIntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Specify the number of Reducers.

        job.setNumReduceTasks(cmdLineArgs.numberOfReducers);

        // Specify the side data to be sent to the mappers and reducers.

        for (int i = 0; i < cmdLineArgs.aggregationIndices.size(); i ++) {

            job.getConfiguration().setInt("aggregationIndices", cmdLineArgs.aggregationIndices.get(i));

        }
        
        job.getConfiguration().setInt("targetIndex", cmdLineArgs.targetIndex);
        job.getConfiguration().setInt("postJoiningIndex", cmdLineArgs.postJoiningIndex);

        // Execute the job.

        long jobStartTimeInMilliseconds = System.currentTimeMillis();
        boolean wasJobSuccessful = job.waitForCompletion(true);
        long jobStopTimeInMilliseconds = System.currentTimeMillis();
        double jobElapsedTimeInSeconds = (jobStopTimeInMilliseconds - jobStartTimeInMilliseconds) / 1000.0;

        LOGGER.info(jobName + " finished in " + jobElapsedTimeInSeconds + " seconds"); 

        if (wasJobSuccessful) {

            return 0;

        }
        else {

            return -1;

        }

    }

}