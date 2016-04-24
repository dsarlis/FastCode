package mapreduce.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public abstract class StarReducer extends Reducer<Text, Text, Text, Text> {

    protected abstract boolean condition(String neighborStr, String u);

    @Override
    protected void reduce(Text node, Iterable<Text> neighbors, Context context)
            throws IOException, InterruptedException {
        String u = node.toString();
        Set<String> neighborsOutput = new HashSet<>();
        String m = computeMin(neighbors, u, neighborsOutput);

        for (String neighbor: neighborsOutput) {
//            System.out.println("Output-> {key: " + neighbor + ", value: " + m + "}");
            context.write(new Text(neighbor), new Text(m));
        }
    }

    private String computeMin(Iterable<Text> neighbors, String u, Set<String> neighborsOutput) {
        String m = u;

        for (Text neighbor: neighbors) {
//            System.out.println("Key: " + u + " value: " + neighbor);
            String neighborStr = neighbor.toString();

            if (neighborStr.compareTo(m) < 0) {
                m = neighborStr;
            }
            if (condition(neighborStr, u)) {
                neighborsOutput.add(neighbor.toString());
            }
        }
        return m;
    }

}
