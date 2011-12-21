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

package org.apache.giraph.examples;

import org.apache.giraph.graph.IntIntNullIntVertex;
import org.apache.giraph.graph.VertexCombiner;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of the HCC algorithm that identifies connected components and assigns each
 * vertex its "component identifier" (the smallest vertex id in the component)
 *
 * The number of supersteps necessary is equal to the maximum diameter of all components + 1
 *
 * The original Hadoop-based variant of this algorithm was proposed by Kang, Charalampos
 * Tsourakakis and Faloutsos in "PEGASUS: Mining Peta-Scale Graphs", 2010
 *
 * http://www.cs.cmu.edu/~ukang/papers/PegasusKAIS.pdf
 */
public class HCCVertex extends IntIntNullIntVertex {

    @Override
    public void compute(Iterator<IntWritable> messages) throws IOException {

        int currentComponentID = getVertexValue().get();
        boolean changed = false;

        if (getSuperstep() == 0) {
            Iterator<IntWritable> edges = iterator();
            while (edges.hasNext()) {
                int neighborID = edges.next().get();
                if (neighborID < currentComponentID) {
                    currentComponentID = neighborID;
                }
            }
            changed = true;
        } else {
            while (messages.hasNext()) {
                int candidateComponentID = messages.next().get();
                if (candidateComponentID < currentComponentID) {
                    currentComponentID = candidateComponentID;
                    changed = true;
                }
            }
        }

        if (changed) {
            IntWritable newComponentID = new IntWritable(currentComponentID);
            setVertexValue(newComponentID);
            Iterator<IntWritable> edges = iterator();
            while (edges.hasNext()) {
                int neighborID = edges.next().get();
                if (neighborID > currentComponentID) {
                    sendMsg(new IntWritable(neighborID), newComponentID);
                }
            }
        }
        voteToHalt();
    }


    public static class HCCVertexCombiner
            extends VertexCombiner<IntWritable,IntWritable> {

        @Override
        public IntWritable combine(IntWritable target,
                List<IntWritable> messages) throws IOException {
            int minimum = Integer.MAX_VALUE;
            for (IntWritable message : messages) {
                if (message.get() < minimum) {
                    minimum = message.get();
                }
            }
            return new IntWritable(minimum);
        }
    }

}
