import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.Iterator;

public class SiteCountReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Iterator<Text> it = values.iterator();

        double sumResponseTime = 0D;
        double sumSize = 0D;
        double count = 0D;

        while(it.hasNext()){
            String[] split = it.next().toString().split(SiteCountMapper.SEPARATOR);
            double respTime = Double.parseDouble(split[0]);
            double bytes = Double.parseDouble(split[1]);
            sumResponseTime += respTime;
            sumSize += bytes;
            count += 1;
        }

        context.write(
                key,
                new Text(sumResponseTime/count + SiteCountMapper.SEPARATOR + sumSize/count)
        );
    }
}
