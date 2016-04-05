package sequential.main;

import java.util.*;

public class ConnectedComponents {
    private List<Integer>[] graph;
    private boolean[] visited;
    private Stack<Integer> stack;
    private int time;
    private int[] lowlink;
    private Map<Integer, String> mapping;
    private List<List<String>> components;

    public List<List<String>> findConnectedComponents(Graph g) {
        graph = g.getGraph();
        mapping = g.getMapping();
        int n = mapping.size();

        visited = new boolean[n];
        stack = new Stack<>();
        time = 0;
        lowlink = new int[n];
        components = new ArrayList<List<String>>();
        for (int u = 0; u < n; u++) {
            if (!visited[u]) {
                dfs(u);
            }
        }
        return components;
    }

    private void dfs(int u) {
        lowlink[u] = time++;
        visited[u] = true;
        stack.add(u);
        boolean isComponentRoot = true;

        for (int v : graph[u]) {
            if (!visited[v])
                dfs(v);
            if (lowlink[u] > lowlink[v]) {
                lowlink[u] = lowlink[v];
                isComponentRoot = false;
            }
        }

        if (isComponentRoot) {
            List<String> component = new ArrayList<String>();
            while (true) {
                int x = stack.pop();
                component.add(mapping.get(x));
                lowlink[x] = Integer.MAX_VALUE;
                if (x == u)
                    break;
            }
            components.add(component);
        }
    }
}
