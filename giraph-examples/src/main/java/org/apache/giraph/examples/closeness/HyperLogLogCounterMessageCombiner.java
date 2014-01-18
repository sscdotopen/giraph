package org.apache.giraph.examples.closeness;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.hadoop.io.IntWritable;

public class HyperLogLogCounterMessageCombiner
  extends MessageCombiner<IntWritable, HyperLogLogCounter> {

  @Override
  public void combine(IntWritable vertexIndex,
                      HyperLogLogCounter originalMessage,
                      HyperLogLogCounter messageToCombine) {
    originalMessage.merge(messageToCombine);
  }

  @Override
  public HyperLogLogCounter createInitialMessage() {
    return new HyperLogLogCounter();
  }
}
