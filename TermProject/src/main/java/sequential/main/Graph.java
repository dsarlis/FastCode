package sequential.main;

import java.util.List;
import java.util.Map;

public class Graph {

    private List<Integer>[] graph;
    Map<Integer, String> mapping;

    public Graph(List<Integer>[] graph, Map<Integer, String> mapping) {
        this.graph = graph;
        this.mapping = mapping;
    }

    public List<Integer>[] getGraph() {
        return graph;
    }

    public Map<Integer, String> getMapping() {
        return mapping;
    }

}
