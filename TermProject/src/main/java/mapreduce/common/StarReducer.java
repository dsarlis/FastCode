package mapreduce.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public abstract class StarReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text node, Iterable<Text> neighbors, Context context)
            throws IOException, InterruptedException {
        String u = node.toString();
        String m = computeMin(neighbors, u);

        reduce(neighbors, context, u, m);
    }

    protected abstract void reduce(Iterable<Text> neighbors, Context context, String u, String m)
            throws IOException, InterruptedException;

    private String computeMin(Iterable<Text> neighbors, String u) {
        String m = u;

        for (Text neighbor: neighbors) {
            String neighborStr = neighbor.toString();

            if (neighborStr.compareTo(m) < 0) {
                m = neighborStr;
            }
        }
        return m;
    }

}
