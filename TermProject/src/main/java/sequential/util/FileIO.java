package sequential.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import sequential.model.Graph;

public class FileIO {

	public static Graph readGraph(InputStream inputStream) {

		Graph result = null;

		try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {

			Map<Integer, List<Integer>> dynamicSizeGraph = new HashMap<>();
			Map<String, Integer> mapping = new HashMap<>();
			String line = null;
			int nodeCounter = 0;
			Map<Integer, String> invertedMapping = new HashMap<>();

			while ((line = bufferedReader.readLine()) != null) {
				if (line.startsWith("#")) {
					continue;
				}
				String[] edge = line.split("\\s+");
				String from = edge[0];
				String to = edge[1];

				if (!mapping.containsKey(from)) {
					invertedMapping.put(nodeCounter, from);
					mapping.put(from, nodeCounter++);
				}
				if (!mapping.containsKey(to)) {
					invertedMapping.put(nodeCounter, to);
					mapping.put(to, nodeCounter++);
				}

				List<Integer> neighbors = Util.getDefaultList(dynamicSizeGraph, mapping.get(from));
				neighbors.add(mapping.get(to));
			}

			@SuppressWarnings("unchecked")
			List<Integer>[] graph = new List[nodeCounter];

			graph = dynamicSizeGraph.values().toArray(graph);
			for (int i = 0; i < nodeCounter; i++) {
				if (graph[i] == null) {
					graph[i] = new ArrayList<Integer>();
				}
			}

			result = new Graph(graph, invertedMapping);

		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}

		return result;
	}

}
