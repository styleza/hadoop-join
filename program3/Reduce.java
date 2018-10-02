import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;


public class Reduce extends Reducer<Object,Text,Text,Text>
{
    private static final Text EMPTY_TEXT = new Text("");
    private Text tmp = new Text();
    private ArrayList<Text> L1 = new ArrayList<Text>();
    private ArrayList<Text> L2 = new ArrayList<Text>();

    public void reduce(Object key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        L1.clear();
        L2.clear();

        while (values.iterator().hasNext()) {
            tmp = values.iterator().next();

            if (tmp.charAt(0) == 'A') {
                L1.add(new Text(tmp.toString().substring(1)));
            } else if (tmp.charAt(0) == 'B') {
                L2.add(new Text(tmp.toString().substring(1)));
            }
        }

        executeJoinLogic(context);
    }

    private void executeJoinLogic(Context context) throws IOException,InterruptedException {

        if (!L1.isEmpty() && !L2.isEmpty()) {
            for (Text A : L1) {
                for (Text B : L2) {

                    String[] partsA = A.toString().split(",");
                    String[] partsB = B.toString().split(",");

                    String str = String.format("%s\t%s\t%s\t%s\t%s", partsA[1], partsA[2], partsB[1], partsB[2], partsB[3]);

                    context.write(new Text(partsA[0]), new Text(str));
                }
            }
        }
    }
}