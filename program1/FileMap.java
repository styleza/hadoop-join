import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public  class FileMap extends Mapper<LongWritable,Text,Text,Text>
{
    Text keyEmit = new Text();
    Text valEmit = new Text();
    private static String commonSeparator;
    public void setup(Context context){
        Configuration configuration = context.getConfiguration();
        commonSeparator=configuration.get("Separator.Common");
    }
    public void map(LongWritable k, Text v, Context context) throws IOException, InterruptedException
    {
        String line=v.toString();
        String[] words=line.split(",");
        // student file parse
        if(words.length==3){
            if(Integer.parseInt(words[2]) <= 1989) return;
        }
        // score file parser
        else if(words.length==4){
            if(Integer.parseInt(words[1]) <= 80 || Integer.parseInt(words[2]) <= 80 || Integer.parseInt(words[3]) <= 80 ) return;
        }
        StringBuilder stringBuilder = new StringBuilder();
        for(int index=1;index<words.length;index++){
            stringBuilder.append(words[index]+(index+1==words.length ? "" : ","));
        }

        keyEmit.set(words[0]);
        valEmit.set(stringBuilder.toString());
        context.write(keyEmit, valEmit);
    }
}