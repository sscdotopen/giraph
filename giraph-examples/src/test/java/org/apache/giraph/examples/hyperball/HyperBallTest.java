package org.apache.giraph.examples.hyperball;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;

import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

public class HyperBallTest {

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
    conf.setComputationClass(HyperBall.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);
    conf.setMessageCombinerClass(HyperLogLogCombiner.class);
    conf.setVertexInputFormatClass(
        HyperBall32TextInputFormat.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    conf.setDoOutputDuringComputation(true);
    conf.enableOneToAllMsgSending();

    // run internally
    Iterable<String> results = InternalVertexRunner.run(conf, graph);

    for (String result : results) {
      System.out.println(result);
    }
  }

}
