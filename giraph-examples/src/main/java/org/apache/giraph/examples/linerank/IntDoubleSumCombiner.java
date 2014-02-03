package org.apache.giraph.examples.linerank;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

public class IntDoubleSumCombiner
    extends MessageCombiner<IntWritable, DoubleWritable> {

  @Override
  public void combine(IntWritable id, DoubleWritable left,
                      DoubleWritable right) {
    left.set(left.get() + right.get());
  }

  @Override
  public DoubleWritable createInitialMessage() {
    return new DoubleWritable();
  }
}
