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
        long start = System.currentTimeMillis();
        Graph g = FileIO.readGraph(Driver.class.getResourceAsStream("/gplus_combined.txt"));
        long end = System.currentTimeMillis();

        System.out.println(String.format("Parsing time: %d minutes", (end - start)/1000/60));
        start = System.currentTimeMillis();
        List<List<String>> components = new ConnectedComponents().findConnectedComponents(g);
        end = System.currentTimeMillis();

        System.out.println(String.format("Runtime for program sequential: %d minutes", (end - start)/1000/60));
        System.out.println(components);
    }

}
