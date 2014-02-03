package org.apache.giraph.examples.linerank;

import com.google.common.collect.UnmodifiableIterator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import java.util.Iterator;

public class IncidentVerticesIterator
    extends UnmodifiableIterator<IntWritable> {

  private final Iterator<Edge<IntWritable, Directions>> edges;
  private IntWritable currentTargetVertex;
  private final IntWritable reusable;

  public IncidentVerticesIterator(Vertex<IntWritable, DoubleWritable,
      Directions> vertex) {
    this(vertex.getEdges().iterator());
  }

  public IncidentVerticesIterator(
      Iterator<Edge<IntWritable, Directions>> edges) {
    this.edges = edges;
    lookAhead();
    reusable = new IntWritable();
  }

  @Override
  public boolean hasNext() {
    return currentTargetVertex != null;
  }

  @Override
  public IntWritable next() {
    reusable.set(currentTargetVertex.get());
    lookAhead();
    return reusable;
  }

  private void lookAhead() {
    currentTargetVertex = null;
    while (edges.hasNext()) {
      Edge<IntWritable, Directions> nextEdge = edges.next();
      if (nextEdge.getValue().isIncident()) {
        currentTargetVertex = nextEdge.getTargetVertexId();
        break;
      }
    }
  }
}
