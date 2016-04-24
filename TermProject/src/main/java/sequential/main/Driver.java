package sequential.main;

import sequential.model.ConnectedComponents;
import sequential.model.Graph;
import sequential.util.FileIO;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;

public class Driver {

	public static void main(String[] args) throws FileNotFoundException {
		long start = System.currentTimeMillis();
		Graph graph = FileIO.readGraph(new FileInputStream(args[0]));
		long end = System.currentTimeMillis();

		System.out.println(String.format("Parsing time: %d milli-seconds", end - start));
		start = System.currentTimeMillis();
		List<List<String>> components = new ConnectedComponents(graph).findConnectedComponents();
		end = System.currentTimeMillis();

		System.out.println(components);

		System.out.println(String.format("Runtime for program sequential: %d milli-seconds", end - start));
		System.out.println(String.format("Result: %d components", components.size()));
	}
}
