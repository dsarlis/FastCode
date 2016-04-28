package mapreduce.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

public abstract class StarReducer extends Reducer<Text, Text, Text, Text> {

    /**
     * Condition for filtering neighbors of node<u>
     * @param neighborStr
     * @param u
     * @return
     */
    protected abstract boolean condition(BigInteger neighborStr, BigInteger u);

    @Override
    protected void reduce(Text node, Iterable<Text> neighbors, Context context)
            throws IOException, InterruptedException {
        String u = node.toString();
        Set<String> neighborsOutput = new HashSet<>();
        // Find minimum labeled node
        String m = computeMin(neighbors, u, neighborsOutput);
        List<String> neighborsSorted = new ArrayList<>(neighborsOutput);
        Collections.sort(neighborsSorted);

        // Output the new edges
        for (String neighbor: neighborsSorted) {
            context.write(new Text(neighbor), new Text(m));
        }
    }

    /**
     * Find the minimum node and filter neighbors
     * @param neighbors
     * @param u
     * @param neighborsOutput
     * @return
     */
    private String computeMin(Iterable<Text> neighbors, String u, Set<String> neighborsOutput) {
        BigInteger m = new BigInteger(u);
        BigInteger uValue = new BigInteger(u);


        for (Text neighbor: neighbors) {
            BigInteger neighborValue = new BigInteger(neighbor.toString());

            if (neighborValue.compareTo(m) < 0) {
                m = neighborValue;
            }
            // Consider only the neighbors that cover a condition (different for small and large star operations)
            if (condition(neighborValue, uValue)) {
                neighborsOutput.add(neighbor.toString());
            }
        }
        return m.toString();
    }

}
