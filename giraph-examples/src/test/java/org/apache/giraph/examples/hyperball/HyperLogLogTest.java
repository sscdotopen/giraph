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

import org.apache.giraph.comm.messages.HyperLogLog;
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
