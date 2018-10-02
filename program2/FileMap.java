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

public  class FileMap extends Mapper<LongWritable,Text,Text,Text>
{
    Text keyEmit = new Text();
    Text valEmit = new Text();
    private static String commonSeparator;
    private static HashMap<String, String> ScoreMap = new HashMap<String, String>();
    private BufferedReader brReader;
    private String scores = "";
    private Text txtMapOutputKey = new Text("");
    private Text txtMapOutputValue = new Text("");

    enum MYCOUNTER {
        RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR, NO_MATCH_RECORD
    }

    public void setup(Context context) throws IOException, InterruptedException {
        for (URI path : context.getCacheFiles()){
            context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
            load(path,context);
        }
    }
    private void load(URI filePath, Context context){
        try {
            String strLineRead = "";

            brReader = new BufferedReader(new FileReader(filePath.toString()));

            while ((strLineRead = brReader.readLine()) != null) {

                String deptFieldArray[] = strLineRead.split(",");

                if (Integer.parseInt(deptFieldArray[1]) <= 80 || Integer.parseInt(deptFieldArray[2]) <= 80 || Integer.parseInt(deptFieldArray[3]) <= 80) {
                    continue;
                }

                ScoreMap.put(deptFieldArray[0].trim(), deptFieldArray[1].trim() + "\t" + deptFieldArray[2].trim() + "\t" + deptFieldArray[3].trim());
            }
            brReader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
            System.out.println(filePath.toString());
        } catch (IOException e) {
            context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
            e.printStackTrace();
        }

    }
    public void map(LongWritable k, Text v, Context context) throws IOException, InterruptedException
    {
        context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);

        if (v.toString().length() > 0) {
            String values[] = v.toString().split(",");

            if (Integer.parseInt(values[2]) <= 1989) {
                return;
            }

            try {
                scores = ScoreMap.get(values[0].toString());
            } finally {
                scores = ((scores == null || scores.equals(null) || scores.equals("")) ? "NOT-FOUND" : scores);
            }

            // Don't write if no matches were found
            if (scores.equals("NOT-FOUND")) {
                context.getCounter(MYCOUNTER.NO_MATCH_RECORD).increment(1);
                return;
            }

            txtMapOutputKey.set(values[0].toString());

            txtMapOutputValue.set(values[1].toString() + "\t"    + values[2].toString() + "\t" + scores);

        }
        context.write(txtMapOutputKey, txtMapOutputValue);
        scores = "";
    }
}