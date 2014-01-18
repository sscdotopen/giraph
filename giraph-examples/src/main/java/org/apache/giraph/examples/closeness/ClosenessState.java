package org.apache.giraph.examples.closeness;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ClosenessState implements Writable {

  private int hops;
  private HyperLogLogCounter counter;

  public ClosenessState() {
    counter = new HyperLogLogCounter();
  }

  public HyperLogLogCounter counter() {
    return counter;
  }

  public void setHops(int hops) {
    this.hops = hops;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(hops);
    counter.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    hops = in.readInt();
    counter.readFields(in);
  }

  @Override
  public String toString() {
    return hops + "\t" + counter.count();
  }
}
