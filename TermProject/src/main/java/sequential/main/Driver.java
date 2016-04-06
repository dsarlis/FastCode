package sequential.main;

import java.util.List;

import sequential.model.ConnectedComponents;
import sequential.model.Graph;
import sequential.util.FileIO;

public class Driver {

	public static void main(String[] args) {

		long start = System.nanoTime();
		Graph graph = FileIO.readGraph(Driver.class.getResourceAsStream("/simple_graph"));
		long end = System.nanoTime();

		System.out.println(String.format("Parsing time: %d nano-seconds", end - start));
		start = System.currentTimeMillis();
		List<List<String>> components = new ConnectedComponents(graph).findConnectedComponents();
		end = System.currentTimeMillis();

		System.out.println(components);

		System.out.println(String.format("Runtime for program sequential: %d nano-econds", end - start));
		System.out.println(String.format("Result: %d components", components.size()));
	}

}
