import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SiteCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private static final Pattern domainPattern = Pattern.compile("url=\"https?://([^/:]+)[/:]");

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {

        Matcher domainMatcher = domainPattern.matcher(value.toString());

        while (domainMatcher.find()) {
            String address = domainMatcher.group(1);
            if(!address.endsWith("globant.com"))
                context.write(new Text(address), ONE);
        }
    }
}

