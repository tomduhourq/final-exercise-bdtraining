import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SiteCountMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Pattern domainPattern =
            Pattern.compile("url=\"https?://([^/:]+)[/:]");

    private static final Pattern responseTimePattern =
            Pattern.compile("fullreqtime=\"([0-9]+)\"");

    private static final Pattern sizePattern =
            Pattern.compile("size=\"([0-9]+)\"");

    public static final String SEPARATOR = "_";


    @Override
    public void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {

        String line = value.toString();
        Matcher matcher = domainPattern.matcher(line);

        if (!matcher.find())
            return;
        String address = matcher.group(1);
        if (address.endsWith("globant.com"))
            return;


        matcher = responseTimePattern.matcher(line);
        if (!matcher.find())
            return;
        String responseTime = matcher.group(1);

        matcher = sizePattern.matcher(line);
        if (!matcher.find())
            return;

        String bytes = matcher.group(1);
        context.write(
                new Text(address),
                new Text(getMapperValue(responseTime, bytes))
        );




    }

    private String getMapperValue(String responseTime, String bytes) {
        StringBuilder builder = new StringBuilder();
        builder.append(responseTime).append(SEPARATOR).append(bytes);
        return builder.toString();
    }
}


