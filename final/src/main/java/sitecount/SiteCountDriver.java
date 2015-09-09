package sitecount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SiteCountDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new SiteCountDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        // Correct input verification
        if (args.length != 2) {
            System.err.println("Usage: SiteCountJob <input path> <output path>");
            return 1;
        }

        Job job = new Job(getConf(), "First Job");
        job.setJarByClass(getClass());

        job.setMapperClass(SiteCountMapper.class);
        job.setReducerClass(SiteCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
