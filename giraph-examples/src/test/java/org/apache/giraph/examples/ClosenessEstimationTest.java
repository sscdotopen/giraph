package org.apache.giraph.examples;


import com.google.common.collect.Maps;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;

import org.apache.giraph.examples.closeness.ClosenessEstimation;
import org.apache.giraph.examples.closeness.ClosenessTextInputFormat;
import org.apache.giraph.examples.closeness.HyperLogLogCounterMessageCombiner;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import java.util.Map;


public class ClosenessEstimationTest {

  @Test
  public void testToyData() throws Exception {

    String[] graph = new String[] {
        "1 2 3",
        "2 4 5",
        "3 4 6",
        "4 1",
        "5 2",
        "6 3" };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(ClosenessEstimation.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);
    conf.setMessageCombinerClass(HyperLogLogCounterMessageCombiner.class);
    conf.setVertexInputFormatClass(ClosenessTextInputFormat.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    conf.setDoOutputDuringComputation(true);

    // run internally
    Iterable<String> results = InternalVertexRunner.run(conf, graph);


    Map<Integer, Map<Integer, Integer>> reachableVerticesPerHop =
        parseResults(results);

    for (String result : results) {
      System.out.println(result);
    }
  }

  private Map<Integer, Map<Integer, Integer>> parseResults(
      Iterable<String> results) {

    Map<Integer, Map<Integer, Integer>> reachableVerticesPerHop =
        Maps.newHashMap();

    for (String result : results) {

      String[] tokens = result.split("\t");
      int hops = Integer.parseInt(tokens[0]);
      int vertex = Integer.parseInt(tokens[1]);
      int numVerticesReached = Integer.parseInt(tokens[2]);

      if (!reachableVerticesPerHop.containsKey(hops)) {
        reachableVerticesPerHop.put(hops, Maps.<Integer, Integer>newHashMap());
      }

      Map<Integer, Integer> reachableVertices =
          reachableVerticesPerHop.get(hops);
      reachableVertices.put(vertex, numVerticesReached);
    }

    return reachableVerticesPerHop;
  }

}
