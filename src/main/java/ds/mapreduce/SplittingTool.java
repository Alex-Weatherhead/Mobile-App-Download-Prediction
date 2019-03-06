package ds.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class SplittingTool extends Configured implements Tool {

    private static final Logger LOGGER = LogManager.getLogger(SplittingTool.class.getName());

    private static final class CmdLineArgs {

        @Option(name="-inputPathname", required=true, usage="The pathname the desired input file or directory.")
        String inputPathname;
    
        @Option(name="-outputPathname", required=true, usage="The pathname of the desired output directory.")
        String outputPathname;
    
        @Option(name = "-overwriteOutputPath", required=false, usage = "Whether or not to overwrite the contents in the output directory if any exist.")
        boolean overwriteOutputPath = false;

        @Option(name = "-numberOfReducers", usage="The number of reduce tasks to use.")
        int numberOfReducers = 1;
        
        @Option(name="-splitIndex", required=true, usage="The indices of the columns on which to group by.")
        int splitIndex;

        @Option(name="-delimeter", required=true, usage="The delimeter with which to split by.")
        String delimeter;

        @Option(name="-limit", required=true, usage="The limit on the number of splits to make.")
        int limit;

        @Option(name="-keepLast", required=true, usage="Whether or not to keep the last split.")
        boolean keepLast;

    }

    private SplittingTool () {}

    public static void main(String args []) throws Exception {

        ToolRunner.run(new SplittingTool(), args);

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
        LOGGER.info("-splitIndex: " + cmdLineArgs.splitIndex);
        LOGGER.info("-delimeter: " + cmdLineArgs.delimeter);
        LOGGER.info("-limit" + cmdLineArgs.limit);
        LOGGER.info("-keepLast" + cmdLineArgs.keepLast);

        // Create the configuration.

        Configuration conf = getConf();

        // Create the job.

        String jobName = SplittingTool.class.getSimpleName();

        Job job = Job.getInstance(conf);
        job.setJarByClass(SplittingTool.class);
        job.setJobName(jobName); 

        // Input path/formatting details.

        Path inputPath = new Path(cmdLineArgs.inputPathname); 
        FileInputFormat.setInputPaths(job, inputPath); 
        job.setInputFormatClass(TextInputFormat.class);

        // Output path/formatting details.

        Path outputPath = new Path(cmdLineArgs.outputPathname);
        FileOutputFormat.setOutputPath(job, outputPath); 
        job.setOutputFormatClass(TextOutputFormat.class);
        if (cmdLineArgs.overwriteOutputPath) { 

            FileSystem.get(conf).delete(outputPath, true); // Recursively delete the contents currently at the outputPath.
        
            LOGGER.info("Overwriting contents at outputPath " + outputPath.toString());

        }
        
        // Specify a Mapper, Partitioner, and Reducer.

        job.setMapperClass(SplittingMapper.class);

        // Specify the (key, value) intermediate/output types for the Mapper/Reducer.

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // Specify the number of Reducers.

        job.setNumReduceTasks(cmdLineArgs.numberOfReducers);

        // Specify the side data to be sent to the mappers and reducers.

        job.getConfiguration().setInt("splitIndex", cmdLineArgs.splitIndex);
        job.getConfiguration().setStrings("delimeter", cmdLineArgs.delimeter);
        job.getConfiguration().setInt("limit", cmdLineArgs.limit);
        job.getConfiguration().setBoolean("keepLast", cmdLineArgs.keepLast);

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