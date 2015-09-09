package topten;

import comparator.MyComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopTenDriver extends Configured implements Tool {

    public static final String TOP_KEY = "Top";
    public static final String TOP_VALUE = "10";

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new TopTenDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        // Correct input verification
        if (args.length != 2) {
            System.err.println("Usage: TopTenJob <input path> <output path>");
            return 1;
        }

        Configuration conf = new Configuration();
        conf.set(TOP_KEY, TOP_VALUE);

        Job job = new Job(conf, "Second Job");
        job.setJarByClass(getClass());

        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);

        job.setSortComparatorClass(MyComparator.DoubleComparator.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
