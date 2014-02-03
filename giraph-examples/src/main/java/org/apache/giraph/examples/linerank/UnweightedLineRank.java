package org.apache.giraph.examples.linerank;


import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class UnweightedLineRank extends BasicComputation<IntWritable,
    DoubleWritable, Directions, DoubleWritable> {

  private static final long NUM_EDGES = 5;

  @Override
  public void compute(Vertex<IntWritable, DoubleWritable, Directions> vertex,
                      Iterable<DoubleWritable> messages) throws IOException {

    double state = 0;

    if (getSuperstep() == 0L) {
      state = 1.0 / NUM_EDGES;
    } else {
      for (DoubleWritable message : messages) {
        state += message.get();
      }

      if (getSuperstep() <= 3L) {
        int numIncidentEdges = numIncidentEdges(vertex);
        state /= numIncidentEdges;
      }
    }

    System.out.println("----> " + vertex.getId().get() + " " + state);
    vertex.getValue().set(state);

    if (getSuperstep() <= 2L) {
      sendMessageToMultipleEdges(
          new IncidentVerticesIterator(vertex.getEdges().iterator()),
          vertex.getValue());
    } else if (getSuperstep() == 3L) {
      sendMessageToAllEdges(vertex, vertex.getValue());
    } else {
      vertex.voteToHalt();
    }

  }

  private int numIncidentEdges(
      Vertex<IntWritable, DoubleWritable, Directions> vertex) {
    int numIncidentEdges = 0;
    for (Edge<IntWritable, Directions> edge : vertex.getEdges()) {
      if (edge.getValue().isIncident()) {
        numIncidentEdges++;
      }
    }
    return numIncidentEdges;
  }

}
