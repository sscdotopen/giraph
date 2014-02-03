package org.apache.giraph.examples.linerank;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

public class UnweightedLineRankTest {

  @Test
  public void testToyData() throws Exception {

    String[] graph = new String[] {
        "1 2:true:true 3:true:true",
        "2 1:true:true 3:false:true",
        "3 1:true:true 2:true:false" };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(UnweightedLineRank.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);
    conf.setMessageCombinerClass(IntDoubleSumCombiner.class);
    conf.setVertexInputFormatClass(LineRankTextInputFormat.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    conf.enableOneToAllMsgSending();

    // run internally
    Iterable<String> results = InternalVertexRunner.run(conf, graph);

    for (String result : results) {
      System.out.println(result);
    }
  }
}
