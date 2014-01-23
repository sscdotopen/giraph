package org.apache.giraph.examples.hyperball;

import org.apache.hadoop.io.LongWritable;

public class HyperBall64 extends HyperBall<LongWritable> {

  @Override
  void initializeCounter(LongWritable id, HyperLogLog counter) {
    counter.observe(id.get());
  }
}
