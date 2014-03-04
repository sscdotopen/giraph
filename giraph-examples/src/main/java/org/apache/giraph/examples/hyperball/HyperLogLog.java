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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/** a HyperLogLog sketch. */
public class HyperLogLog implements Writable, Cloneable {

  private static final int NUMBER_OF_BUCKETS = 16;
  private static final double ALPHA_TIMES_MSQUARED =
      0.673 * NUMBER_OF_BUCKETS * NUMBER_OF_BUCKETS;
  private static final double ESTIMATION_THRESHOLD = 2.5 * NUMBER_OF_BUCKETS;

  //TODO should be configurable
  private static final int SEED = 0xdead;

  private byte[] buckets;
  
  public HyperLogLog() {
    this(new byte[NUMBER_OF_BUCKETS]);
  }

  public HyperLogLog(byte[] buckets) {
    this.buckets = buckets;
  }


  public void observe(long item) {
    setRegister(MurmurHash3.hash(item, SEED));
  }

  private void setRegister(int hash) {
    // last 4 bits as bucket index
    int mask = NUMBER_OF_BUCKETS - 1;
    int bucketIndex = hash & mask;

    // throw away last 4 bits
    hash >>= 4;
    // make sure the 4 new zeroes don't impact estimate
    hash |= 0xf0000000;
    // hash has now 28 significant bits left
    buckets[bucketIndex] = (byte) (Integer.numberOfTrailingZeros(hash) + 1L);
  }

  public long count() {

    double sum = 0.0;
    for (byte bucket : buckets) {
      sum += Math.pow(2.0, -bucket);
    }
    long estimate = (long) (ALPHA_TIMES_MSQUARED * (1.0 / sum));

    if (estimate < ESTIMATION_THRESHOLD) {
      /* look for empty buckets */
      int numEmptyBuckets = 0;
      for (byte bucket : buckets) {
        if (bucket == 0) {
          numEmptyBuckets++;
        }
      }

      if (numEmptyBuckets != 0) {
        estimate = (long) (NUMBER_OF_BUCKETS *
            Math.log((double) NUMBER_OF_BUCKETS / (double) numEmptyBuckets));
      }
    }

    return estimate;
  }
  
  public void merge(HyperLogLog other) {
    /* take the maximum of each bucket pair */
    for (int index = 0; index < buckets.length; index++) {
      if (buckets[index] < other.buckets[index]) {
        buckets[index] = other.buckets[index];
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.write(buckets);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    in.readFully(buckets);
  }

  @Override
  protected HyperLogLog clone() {
    byte[] clonedBuckets = new byte[NUMBER_OF_BUCKETS];
    System.arraycopy(buckets, 0, clonedBuckets, 0, buckets.length);
    return new HyperLogLog(clonedBuckets);
  }

  @Override
  public String toString() {
    return String.valueOf(count());
  }
}
