package org.apache.giraph.examples.hyperball;

import org.junit.Test;

import java.util.BitSet;
import java.util.Random;

public class HyperLogLogTest {

  @Test
  public void countMultipleTimes() {
    for (int n = 0; n < 50; n++) {
      counting();
    }
  }

  public void counting() {

    int n = 50000000;

    int tries = 50000000;

    Random random = new Random();
    BitSet seen = new BitSet(n);

    HyperLogLog counter = new HyperLogLog();

    int c = 0;
    while (c++ < tries) {
      int item = random.nextInt(n);

      seen.set(item);
      HyperLogLog itemCounter = new HyperLogLog();
      itemCounter.observe(item);

      counter.merge(itemCounter);
    }

    long estimate = counter.count();
    int truth = seen.cardinality();

    double error = Math.abs(1 - ( (double) estimate / (double) truth));

    System.out.println("-->" + counter.count() + " / " + seen.cardinality() + " " + error);

  }
}
