package org.apache.giraph.examples.closeness;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/** a HyperLogLog sketch. */
public class HyperLogLogCounter implements Writable {
  
  /* must be a power of two */
  private static final int NUMBER_OF_BUCKETS = 64;
  private static final double ALPHA_TIMES_MSQUARED =
      0.709 * NUMBER_OF_BUCKETS * NUMBER_OF_BUCKETS;
  
  private byte[] buckets;
  
  public HyperLogLogCounter() {
    buckets = new byte[NUMBER_OF_BUCKETS];
  }

  public void observe(long n) {
    /* Murmur 2.0 hash START */
    long m = 0xc6a4a7935bd1e995L;
    int r = 47;
          
    byte[] data = new byte[8];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) n;
      n >>= 8;
    }

    long hash = (1L & 0xffffffffL) ^ (8 * m);
    long k =  ( (long) data[0] & 0xff)        + (((long) data[1] & 0xff) << 8)
            + (((long) data[2] & 0xff) << 16) + (((long) data[3] & 0xff) << 24)
            + (((long) data[4] & 0xff) << 32) + (((long) data[5] & 0xff) << 40)
            + (((long) data[6] & 0xff) << 48) + (((long) data[7] & 0xff) << 56);
                
    k *= m;
    k ^= k >>> r;
    k *= m;
                
    hash ^= k;
    hash *= m;
    hash *= m;
    hash ^= hash >>> r;
    hash *= m;
    hash ^= hash >>> r;
    /* Murmur 2.0 hash END */
    
    /* last 6 bits as bucket index */
    int mask = NUMBER_OF_BUCKETS - 1;
    int index = (int) (hash & mask);
    
    /* throw away last 6 bits */
    hash >>= 6;
    /* make sure the 6 new zeroes don't impact estimate */
    hash |= 0xfc00000000000000L;
    /* hash has now 58 significant bits left */
    buckets[index] = (byte) (Long.numberOfTrailingZeros(hash) + 1L);
  }
  
  public long count() {

    double sum = 0.0;
    for (int index = 0; index < buckets.length; index++) {
      sum += Math.pow(2.0, -buckets[index]);
    }
    long estimate = (long) (ALPHA_TIMES_MSQUARED * (1.0 / sum));

    if (estimate < 2.5 * NUMBER_OF_BUCKETS) {
      /* look for empty buckets */
      int V = 0;
      for (int index = 0; index < buckets.length; index++) {
        if (buckets[index] == 0) {
          V++;
        }
      }

      if (V != 0) {
        estimate = (long) (NUMBER_OF_BUCKETS *
            Math.log((double) NUMBER_OF_BUCKETS / (double) V));
      }
    }

    return estimate;
  }
  
  public void merge(HyperLogLogCounter other) {
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
  public String toString() {
    return String.valueOf(count());
  }
}
