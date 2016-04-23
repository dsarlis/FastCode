package mapreduce.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class StarReducer extends Reducer<Text, Text, Text, Text> {

    protected abstract boolean condition(String neighborStr, String u);

    @Override
    protected void reduce(Text node, Iterable<Text> neighbors, Context context)
            throws IOException, InterruptedException {
        String u = node.toString();
        List<Text> neighborsOutput = new ArrayList<>();
        String m = computeMin(neighbors, u, neighborsOutput);

        for (Text neighbor: neighborsOutput) {
            context.write(neighbor, new Text(m));
        }
    }

    private String computeMin(Iterable<Text> neighbors, String u, List<Text> neighborsOutput) {
        String m = u;

        for (Text neighbor: neighbors) {
            String neighborStr = neighbor.toString();

            if (neighborStr.compareTo(m) < 0) {
                m = neighborStr;
            }
            if (condition(neighborStr, u)) {
                neighborsOutput.add(neighbor);
            }
        }
        return m;
    }

}
