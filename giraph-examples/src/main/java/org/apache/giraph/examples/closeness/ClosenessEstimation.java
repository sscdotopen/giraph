package org.apache.giraph.examples.closeness;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class ClosenessEstimation extends BasicComputation<IntWritable,
    ClosenessState, NullWritable, HyperLogLogCounter> {

  @Override
  public void compute(
      Vertex<IntWritable, ClosenessState, NullWritable> vertex,
      Iterable<HyperLogLogCounter> neighborCounters) throws IOException {

    HyperLogLogCounter counter = vertex.getValue().counter();

    if (getSuperstep() == 0L) {

      counter.observe(vertex.getId().get());
      sendMessageToAllEdges(vertex, counter);

    } else {

      long numSeenBefore = counter.count();

      for (HyperLogLogCounter neighborCounter : neighborCounters) {
        counter.merge(neighborCounter);
      }

      long numNewlySeen = counter.count() - numSeenBefore;

      if (numNewlySeen > 0) {
        vertex.getValue().setHops((int) getSuperstep());
        sendMessageToAllEdges(vertex, counter);
      }
    }

    vertex.voteToHalt();
  }

}
