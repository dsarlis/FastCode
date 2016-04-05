package sequential.util;

import sequential.main.Graph;
import sequential.main.Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileIO {

    public static Graph readGraph(InputStream inputStream) {
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
            Map<Integer, List<Integer>> dynamicSizeGraph = new HashMap<Integer, List<Integer>>();
            Map<Integer, Integer> mapping = new HashMap<Integer, Integer>();
            String line = null;
            int nodeCounter = 0;

            while ((line = bufferedReader.readLine()) != null) {
                String[] edge = line.split("\\s+");
                int from = Integer.parseInt(edge[0]);
                int to = Integer.parseInt(edge[1]);

                if (!mapping.containsKey(from)) {
                    mapping.put(from, nodeCounter++);
                }
                if (!mapping.containsKey(to)) {
                    mapping.put(to, nodeCounter++);
                }
                List<Integer> neighbors = Util.getDefaultList(dynamicSizeGraph, mapping.get(from));

                neighbors.add(mapping.get(to));
            }
            List<Integer>[] graph  = new List[dynamicSizeGraph.size()];
            int[] invertedMapping = new int[graph.length];

            graph = dynamicSizeGraph.values().toArray(graph);
            for (Map.Entry<Integer, Integer> entry: mapping.entrySet()) {
                invertedMapping[entry.getValue()] = entry.getKey();
            }
            return new Graph(graph, invertedMapping);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}
