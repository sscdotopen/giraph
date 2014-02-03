package org.apache.giraph.examples.linerank;


import com.google.common.collect.Lists;
import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.ReusableEdge;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class IncidentVerticesIteratorTest {

  @Test
  public void test() {

    List<Edge<IntWritable, Directions>> edges = Lists.newArrayList();

    edges.add(edge(0, false, true));
    edges.add(edge(1, true, true));
    edges.add(edge(2, false, true));
    edges.add(edge(3, true, false));
    edges.add(edge(4, false, false));
    edges.add(edge(5, true, true));
    edges.add(edge(6, false, true));
    edges.add(edge(7, true, true));
    edges.add(edge(8, false, true));

    testIterator(new int[] { 1, 3, 5, 7 }, edges);
  }

  @Test
  public void test2() {

    List<Edge<IntWritable, Directions>> edges = Lists.newArrayList();

    edges.add(edge(0, true, false));
    edges.add(edge(1, true, true));
    edges.add(edge(2, false, true));
    edges.add(edge(3, true, false));
    edges.add(edge(4, false, false));
    edges.add(edge(5, true, true));
    edges.add(edge(6, false, true));
    edges.add(edge(7, true, true));
    edges.add(edge(8, true, true));
    edges.add(edge(9, true, true));

    testIterator(new int[] { 0, 1, 3, 5, 7, 8, 9 }, edges);
  }

  @Test
  public void test3() {

    List<Edge<IntWritable, Directions>> edges = Lists.newArrayList();

    edges.add(edge(0, false, false));
    edges.add(edge(1, true, true));
    edges.add(edge(2, false, true));
    edges.add(edge(3, true, false));
    edges.add(edge(4, false, false));
    edges.add(edge(5, false, true));
    edges.add(edge(6, false, true));
    edges.add(edge(7, false, true));
    edges.add(edge(8, true, true));
    edges.add(edge(9, true, true));

    testIterator(new int[] { 1, 3, 8, 9 }, edges);
  }

  private void testIterator(int[] expectedVertices,
                            Iterable<Edge<IntWritable, Directions>> edges) {
    Iterator<IntWritable> incidentVerticesIterator =
        new IncidentVerticesIterator(edges.iterator());

    List<Integer> incidentVertices = Lists.newArrayList();
    while (incidentVerticesIterator.hasNext()) {
      incidentVertices.add(incidentVerticesIterator.next().get());
    }

    assertEquals(expectedVertices.length, incidentVertices.size());

    for (int n = 0; n < expectedVertices.length; n++) {
      assertEquals(expectedVertices[n], incidentVertices.get(n).intValue());
    }
  }


  private Edge<IntWritable, Directions> edge(int target, boolean incident,
                                             boolean adjacent) {
    ReusableEdge<IntWritable, Directions> edge = new DefaultEdge();
    edge.setTargetVertexId(new IntWritable(target));

    edge.setValue(new Directions(incident, adjacent));

    return edge;
  }
}
