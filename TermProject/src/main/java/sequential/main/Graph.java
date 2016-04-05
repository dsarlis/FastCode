package sequential.main;

import java.util.List;

public class Graph {

    private List<Integer>[] graph;
    int[] mapping;

    public Graph(List<Integer>[] graph, int[] mapping) {
        this.graph = graph;
        this.mapping = mapping;
    }

    public List<Integer>[] getGraph() {
        return graph;
    }

    public int[] getMapping() {
        return mapping;
    }

}
