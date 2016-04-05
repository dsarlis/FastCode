package sequential.main;

import sequential.util.FileIO;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Driver {

    public static void main(String[] args) {
        Graph g = FileIO.readGraph(Driver.class.getResourceAsStream("/medium_graph"));
        List<List<Integer>> components = new ConnectedComponents().findConnectedComponents(g);

        System.out.println(components);
    }

}
