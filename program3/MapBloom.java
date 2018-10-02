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
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public  class MapBloom  extends Mapper<Object,Text,Text,Text>
{
    private BloomFilter bfilter = new BloomFilter(2_000_000, 7, Hash.MURMUR_HASH);
    private Text outkey = new Text();
    private Text outvalue = new Text();
    private BufferedReader brReader;


    @Override
    public void setup(Context context) throws IOException {
        URI[] files = context.getCacheFiles();

        for (URI eachPath : files) {
            loadScoresBloom(eachPath, context);
        }
    }

    private void loadScoresBloom(URI filePath, Context context) throws IOException {

        String strLineRead = "";

        try {
            brReader = new BufferedReader(new FileReader(filePath.toString()));

            while ((strLineRead = brReader.readLine()) != null) {

                String deptFieldArray[] = strLineRead.split(",");

                if (Integer.parseInt(deptFieldArray[1]) <= 80 ||
                        Integer.parseInt(deptFieldArray[2]) <= 80 ||
                        Integer.parseInt(deptFieldArray[3]) <= 80) {
                    continue;
                }

                if (deptFieldArray[0].equals(null)) {
                    continue;
                }

                Key filterKey = new Key(deptFieldArray[0].trim().getBytes());
                bfilter.add(filterKey);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (brReader != null) {
                brReader.close();
            }
        }
    }

    public void map(Object k, Text v, Context context) throws IOException, InterruptedException {

        String record = v.toString();
        String[] parts = record.split(",");

        for (int x = 1; x < 4; x++) {
            if (Integer.parseInt(parts[x]) <= 80) {
                return;
            }
        }

        String studentId = parts[0];

        if (studentId == null) {
            return;
        }

        if (bfilter.membershipTest(new Key(studentId.getBytes()))) {
            outkey.set(studentId);
            outvalue.set("B" + v.toString());
            context.write(outkey, outvalue);
        }
    }
}