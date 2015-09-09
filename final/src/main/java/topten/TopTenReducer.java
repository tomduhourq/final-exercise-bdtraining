package topten;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import sitecount.SiteCountMapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


// At this point, keys should be arriving in descendant order
public class TopTenReducer extends Reducer<DoubleWritable, Text, Text, Text> {

    @Override
    public void reduce(DoubleWritable count, Iterable<Text> values, Context context)
            throws java.io.IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        Integer current = Integer.parseInt(conf.get(TopTenDriver.TOP_KEY));

        Iterator<Text> it = values.iterator();

        List<String[]> sameCountSites = new ArrayList<>();

        while(it.hasNext() && current > 0) {
            String[] metrics = it.next().toString().split(SiteCountMapper.SEPARATOR);
            sameCountSites.add(metrics);
            current --;
        }

        conf.set(TopTenDriver.TOP_KEY, current.toString());

        for(String[] siteMetrics : sameCountSites) {

            String site = siteMetrics[0];
            String avgReqTime = siteMetrics[1];
            String avgBytesSent = siteMetrics[2];

            context.write(
                    new Text(site),
                    new Text(count.toString() + SiteCountMapper.SEPARATOR +
                            avgReqTime + SiteCountMapper.SEPARATOR +
                            avgBytesSent)
            );
        }
    }
}
