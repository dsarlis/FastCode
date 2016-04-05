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
            Map<String, Integer> mapping = new HashMap<String, Integer>();
            String line = null;
            int nodeCounter = 0;

            while ((line = bufferedReader.readLine()) != null) {
                String[] edge = line.split("\\s+");
                String from = edge[0];
                String to = edge[1];

                if (!mapping.containsKey(from)) {
                    mapping.put(from, nodeCounter++);
                }
                if (!mapping.containsKey(to)) {
                    mapping.put(to, nodeCounter++);
                }
                List<Integer> neighbors = Util.getDefaultList(dynamicSizeGraph, mapping.get(from));

                neighbors.add(mapping.get(to));
            }
            List<Integer>[] graph  = new List[nodeCounter];
            Map<Integer, String> invertedMapping = new HashMap<Integer, String>();

            graph = dynamicSizeGraph.values().toArray(graph);
            for (int i = 0; i < nodeCounter; i++) {
                if (graph[i] == null) {
                    graph[i] = new ArrayList<Integer>();
                }
            }
            for (Map.Entry<String, Integer> entry: mapping.entrySet()) {
                invertedMapping.put(entry.getValue(), entry.getKey());
            }
            return new Graph(graph, invertedMapping);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}
