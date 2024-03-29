##18-645 How to Write Fast Code Term Project

###Members

* Dimitris Sarlis
* Federico Ponte
* Gaurav Walia

###Requirements

* Maven
* Java 1.8

###How to build

```bash
maven clean package

```

###Graph file format

Graph file are expected to have the following format:

```
node_from1 node_to1
node_from2 node_to2
...
node_fromn node_ton
```

###How to run

####Sequential

Use the sequential.jar file located in the target folder.

```bash
java -jar sequential.jar graph_file_path
```

It will output the time and number of components in the graph.

Example:

```bash
java -jar sequential.jar simple_graph
```

####Map Reduce

Use the mapreduce.jar file.

You will need to use a Hadoop cluster and run it as a Job (Custom Jar in Amazon).

It takes the following arguments:

```bash
-program [hashToMin|hashToAll|twoPhase] -input graph_location_input -output output_folder -tmpdir temp_folder
```

Example

```bash
-program hashToMin -input s3://18645-termproject-input/big_graph -output s3://18645-termproject-output/hashtomin-big -tmpdir tmp
```

###Scripts

We have three scripts:

* **create_cluster.sh**: Creates the Amazon EMR with speacial configuration we were using.
* **generate-graph.py**: Creates random graph using networkx from Python. Takes three arguments: number of nodes, number of edges and output file.
* **manual-generate-graph.py** : Creates a random graph using two nested fors. Takes three arguments: number of nodes, output file and probability of generating edges.
