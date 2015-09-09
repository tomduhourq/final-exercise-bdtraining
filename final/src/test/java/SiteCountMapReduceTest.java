import com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import sitecount.SiteCountMapper;
import sitecount.SiteCountReducer;

import java.io.IOException;
import java.util.List;

public class SiteCountMapReduceTest {

    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;
    String line1 = "2015:01:13-12:29:20 AR-BADC-FAST-01 httpproxy[27983]: id=\"0001\" severity=\"info\" sys=\"SecureWeb\" sub=\"http\" name=\"http access\" action=\"pass\" method=\"GET\" srcip=\"10.20.6.132\" dstip=\"23.12.161.145\" user=\"\" ad_domain=\"\" statuscode=\"304\" cached=\"0\" profile=\"REF_DefaultHTTPProfile (Default Web Filter Profile)\" filteraction=\"REF_DefaultHTTPCFFAction (Default content filter action)\" size=\"0\" request=\"0x2d5755d8\" url=\"http://argentina.emc.com/R1/assetsmin/js/libs/cufon-yui.js\" exceptions=\"\" error=\"\" authtime=\"0\" dnstime=\"1\" cattime=\"316486\" avscantime=\"0\" fullreqtime=\"891567\" device=\"0\" auth=\"0\" category=\"105,175\" reputation=\"trusted\" categoryname=\"Business,Software/Hardware\"\n";
    String line2 = "2015:01:13-00:15:32 AR-BADC-FAST-01 httpproxy[27983]: id=\"0002\" severity=\"info\" sys=\"SecureWeb\" sub=\"http\" name=\"web request blocked\" action=\"block\" method=\"CONNECT\" srcip=\"10.20.11.196\" dstip=\"\" user=\"\" ad_domain=\"\" statuscode=\"403\" cached=\"0\" profile=\"REF_DefaultHTTPProfile (Default Web Filter Profile)\" filteraction=\"REF_DefaultHTTPCFFAction (Default content filter action)\" size=\"2529\" request=\"0x14dc2e70\" url=\"http://argentina.emc.com/R1/assetsmin/js/libs/cufon-yui.js\" exceptions=\"\" error=\"Target service not allowed\" authtime=\"0\" dnstime=\"0\" cattime=\"0\" avscantime=\"0\" fullreqtime=\"6781\" device=\"0\" auth=\"0\"\n";

    @Before
    public void setUp() {
        SiteCountMapper mapper = new SiteCountMapper();
        SiteCountReducer reducer = new SiteCountReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testMapper() throws IOException {
        LongWritable inKey1 =  new LongWritable(0);
        Text inValue1 = new Text(line1);
        LongWritable inKey2 = new LongWritable(1);
        Text inValue2 = new Text(line2);

        mapDriver.withInput(inKey1, inValue1);
        mapDriver.withInput(inKey2, inValue2);

        Text outKey1 = new Text("argentina.emc.com");
        Text outValue1 = new Text("891567_0");
        Text outKey2 = new Text("argentina.emc.com");
        Text outValue2 = new Text("6781_2529");
        mapDriver.withOutput(outKey1, outValue1);
        mapDriver.withOutput(outKey2, outValue2);
        mapDriver.runTest( true );
    }

    @Test
    public void testReducer() throws IOException {
        Text inKey = new Text("argentina.emc.com");
        Text inVal1 = new Text("891567_0");
        Text inVal2 = new Text("6781_2529");
        List<Text> inListValues = Lists.newArrayList(inVal1, inVal2);
        reduceDriver.withInput(inKey, inListValues);

        Text outKey = new Text("2.0");
        Text outValue = new Text("argentina.emc.com_449174.0_1264.5");
        reduceDriver.withOutput(outKey, outValue);
        reduceDriver.runTest();
    }
}
