package org.apache.giraph.examples.hyperball;


import org.apache.hadoop.io.IntWritable;

public class HyperBall32 extends HyperBall<IntWritable> {

  @Override
  void initializeCounter(IntWritable vertexID, HyperLogLog counter) {
    counter.observe(vertexID.get());
  }
}
