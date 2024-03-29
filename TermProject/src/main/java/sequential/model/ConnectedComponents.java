package sequential.model;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public class ConnectedComponents {

	private Graph graph;
	private boolean[] visited;
	private Deque<Integer> stack;
	private int time;
	private int[] lowlink;
	private List<List<String>> components;
	private int numberOfNodes;

	public ConnectedComponents(Graph g) {

		this.graph = g;
		numberOfNodes = g.getMapping().size();
		visited = new boolean[numberOfNodes];
		stack = new ArrayDeque<>();
		time = 0;
		lowlink = new int[numberOfNodes];
		components = new ArrayList<>();
	}

    /**
     * Find the connected components of a graph by repeatedly executing a DFS search
     * @return
     */
	public List<List<String>> findConnectedComponents() {
		for (int u = 0; u < numberOfNodes; u++) {
			if (!visited[u]) {
				dfs(u);
			}
		}
		return components;
	}

	/**
	 * Traverse the graph using DFS and mark nodes that belong to the same connected component
     * This is based on Tarjan's algorithm: https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
	 * @param u
     */
	private void dfs(int u) {

		lowlink[u] = time++;
		visited[u] = true;
		stack.push(u);
		boolean isComponentRoot = true;

		for (int v : graph.getGraph()[u]) {
			if (!visited[v]) {
				dfs(v);
			}
			if (lowlink[u] > lowlink[v]) {
				lowlink[u] = lowlink[v];
				isComponentRoot = false;
			}
		}

		if (isComponentRoot) {
			List<String> component = new ArrayList<>();
			while (true) {
				int x = stack.pop();
				component.add(graph.getMapping().get(x));
				lowlink[x] = Integer.MAX_VALUE;
				if (x == u) {
					break;
				}
			}
			components.add(component);
		}
	}
}
