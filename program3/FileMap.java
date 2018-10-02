import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import java.net.URI;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public  class FileMap extends Mapper<Object,Text,Text,Text>
{
    Text keyEmit = new Text();
    Text valEmit = new Text();
    private static String commonSeparator;
    private static HashMap<String, String> ScoreMap = new HashMap<String, String>();
    private BufferedReader brReader;
    private String scores = "";
    private Text txtMapOutputKey = new Text("");
    private Text txtMapOutputValue = new Text("");

    public void map(Object k, Text v, Context context) throws IOException, InterruptedException
    {
        String record = v.toString();
        String[] parts = record.split(",");
        String sid = parts[0];

        if (sid == null) {
            return;
        }

        String yob = parts[2];

        if (yob == null) {
            return;
        }

        if (Integer.parseInt(yob) > 1989) {
            txtMapOutputKey.set(sid);
            txtMapOutputValue.set("A" + v.toString());
            context.write(txtMapOutputKey, txtMapOutputValue);
        }
    }
}