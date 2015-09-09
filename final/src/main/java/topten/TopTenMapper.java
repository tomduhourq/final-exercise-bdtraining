package topten;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopTenMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    static final String SEPARATOR = "\t";

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {

        String[] val = value.toString().split(SEPARATOR);
        double count = Double.parseDouble(val[0]);
        String metrics = val[1];

        context.write(
           new DoubleWritable(count),
           new Text(metrics)
        );
    }
}
