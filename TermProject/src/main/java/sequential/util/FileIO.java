package sequential.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileIO {
    public static List<Integer>[]  readGraph(InputStream inputStream) {
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
            Map<Integer, List<Integer>> dynamicSizeGraph = new HashMap<Integer, List<Integer>>();
            String line = null;

            while ((line = bufferedReader.readLine()) != null) {
                String[] edge = line.split("\\s+");
                int from = Integer.parseInt(edge[0]);
                int to = Integer.parseInt(edge[1]);

                List<Integer> neighbors = dynamicSizeGraph.get(from);
                if (neighbors == null) {
                    neighbors = new ArrayList<Integer>();
                    dynamicSizeGraph.put(from, neighbors);
                }
                neighbors.add(to);
            }
            List<Integer>[] graph  = new List[dynamicSizeGraph.size()];
            return dynamicSizeGraph.values().toArray(graph);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
