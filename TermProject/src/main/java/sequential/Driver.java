package sequential;

import java.util.ArrayList;
import java.util.List;

public class Driver {
    public static void main(String[] args) {
        List<Integer>[] g = new List[13];
        for (int i = 0; i < g.length; i++)
            g[i] = new ArrayList<>();

        g[0].add(1);
        g[0].add(2);
        g[1].add(0);
        g[1].add(2);
        g[2].add(0);
        g[2].add(1);
        g[2].add(3);
        g[3].add(2);

        g[4].add(5);
        g[4].add(6);
        g[5].add(4);
        g[6].add(4);

        g[7].add(8);
        g[8].add(7);
        g[8].add(9);
        g[8].add(10);
        g[8].add(11);
        g[9].add(8);
        g[10].add(8);
        g[10].add(11);
        g[11].add(8);
        g[11].add(10);
        g[11].add(12);
        g[12].add(11);

        List<List<Integer>> components = new ConnectedComponents().findConnectedComponents(g);
        System.out.println(components);
    }
}
