package mapreduce.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

public abstract class StarReducer extends Reducer<Text, Text, Text, Text> {

    protected abstract boolean condition(BigInteger neighborStr, BigInteger u);

    @Override
    protected void reduce(Text node, Iterable<Text> neighbors, Context context)
            throws IOException, InterruptedException {
        String u = node.toString();
        Set<String> neighborsOutput = new HashSet<>();
        String m = computeMin(neighbors, u, neighborsOutput);
        List<String> neighborsSorted = new ArrayList<>(neighborsOutput);
        Collections.sort(neighborsSorted);

        for (String neighbor: neighborsSorted) {
//            System.out.println("Output-> {key: " + neighbor + ", value: " + m + "}");
            context.write(new Text(neighbor), new Text(m));
        }
    }

    private String computeMin(Iterable<Text> neighbors, String u, Set<String> neighborsOutput) {
        BigInteger m = new BigInteger(u);
        BigInteger uValue = new BigInteger(u);


        for (Text neighbor: neighbors) {
//            System.out.println("Key: " + u + " value: " + neighbor);
            BigInteger neighborValue = new BigInteger(neighbor.toString());

            if (neighborValue.compareTo(m) < 0) {
                m = neighborValue;
            }
            if (condition(neighborValue, uValue)) {
                neighborsOutput.add(neighbor.toString());
            }
        }
        return m.toString();
    }

}
