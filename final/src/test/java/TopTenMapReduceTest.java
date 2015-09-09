import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import topten.TopTenDriver;
import topten.TopTenMapper;
import topten.TopTenReducer;

import java.io.IOException;
import java.util.List;

public class TopTenMapReduceTest {

    MapDriver<LongWritable, Text, DoubleWritable, Text> mapDriver;
    ReduceDriver<DoubleWritable, Text, Text, Text> reduceDriver;
    String line1 = "10.0\targentina.emc.com_12984.1_321.5";
    String line2 = "10.0\tmtalk.google.com_422.1_544.1";

    @Before
    public void setUp() {
        Configuration conf = new Configuration();
        conf.set(TopTenDriver.TOP_KEY, TopTenDriver.TOP_VALUE);
        TopTenMapper mapper = new TopTenMapper();
        TopTenReducer reducer = new TopTenReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver
                .newReduceDriver(reducer)
                .withConfiguration(conf);
    }

    @Test
    public void testSameKeyMapper() throws IOException {
        LongWritable inKey1 =  new LongWritable(1);
        Text inValue1 = new Text(line1);
        LongWritable inKey2 = new LongWritable(1);
        Text inValue2 = new Text(line2);

        mapDriver.withInput(inKey1, inValue1);
        mapDriver.withInput(inKey2, inValue2);

        DoubleWritable outKey1 = new DoubleWritable(10.0);
        Text outValue1 = new Text("argentina.emc.com_12984.1_321.5");
        DoubleWritable outKey2 = new DoubleWritable(10.0);
        Text outValue2 = new Text("mtalk.google.com_422.1_544.1");
        mapDriver.withOutput(outKey1, outValue1);
        mapDriver.withOutput(outKey2, outValue2);
        mapDriver.runTest( true );
    }

    @Test
    public void testSameKeyReducer() throws IOException {
        DoubleWritable inKey = new DoubleWritable(10.0);
        Text inVal1 = new Text("argentina.emc.com_12984.1_321.5");
        Text inVal2 = new Text("mtalk.google.com_422.1_544.1");
        List<Text> inListValues = Lists.newArrayList(inVal1, inVal2);
        reduceDriver.withInput(inKey, inListValues);

        Text outKey1 = new Text("argentina.emc.com");
        Text outValue1 = new Text("10.0_12984.1_321.5");
        Text outKey2 = new Text("mtalk.google.com");
        Text outValue2 = new Text("10.0_422.1_544.1");
        reduceDriver.withOutput(outKey1, outValue1);
        reduceDriver.withOutput(outKey2, outValue2);
        reduceDriver.runTest();
    }
}
