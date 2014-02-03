/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.examples.hyperball;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EstimatedNF implements Writable {

  private HyperLogLog counter;

  private IntArrayList nf = new IntArrayList();

  public EstimatedNF() {
    counter = new HyperLogLog();
  }

  public HyperLogLog counter() {
    return counter;
  }

  public void registerEstimate(int hops, int numReachableVertices) {
    nf.add(hops);
    nf.add(numReachableVertices);
  }

  @Override
  public void write(DataOutput out) throws IOException {

    counter.write(out);

    int size = nf.size();
    out.writeInt(size);
    for (int n = 0; n < size; n++) {
      out.writeInt(nf.getInt(n));
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    counter.readFields(in);
    int size = in.readInt();
    int[] elems = new int[size];
    for (int n = 0; n < size; n++) {
      elems[n] = in.readInt();
    }
    nf = new IntArrayList(elems);
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    int n = 0;
    int size = nf.size();
    while (n < size) {
      int hop = nf.getInt(n++);
      int numReachableVertices = nf.getInt(n++);
      buffer.append(hop).append(":")
            .append(numReachableVertices).append(";");
    }
    return buffer.toString();
  }
}
