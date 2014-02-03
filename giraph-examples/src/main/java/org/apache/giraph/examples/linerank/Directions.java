package org.apache.giraph.examples.linerank;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Directions implements Writable {

  private boolean incident;
  private boolean adjacent;

  public Directions() {}

  public Directions(boolean incident, boolean adjacent) {
    this.incident = incident;
    this.adjacent = adjacent;
  }

  public boolean isIncident() {
    return incident;
  }

  public boolean isAdjacent() {
    return adjacent;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(incident);
    out.writeBoolean(adjacent);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    incident = in.readBoolean();
    adjacent = in.readBoolean();
  }
}
