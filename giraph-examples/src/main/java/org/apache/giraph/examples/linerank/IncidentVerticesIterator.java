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

package org.apache.giraph.examples.linerank;

import com.google.common.collect.UnmodifiableIterator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import java.util.Iterator;

public class IncidentVerticesIterator
    extends UnmodifiableIterator<IntWritable> {

  private final Iterator<Edge<IntWritable, Directions>> edges;
  private IntWritable currentTargetVertex;
  private final IntWritable reusable;

  public IncidentVerticesIterator(Vertex<IntWritable, DoubleWritable,
      Directions> vertex) {
    this(vertex.getEdges().iterator());
  }

  public IncidentVerticesIterator(
      Iterator<Edge<IntWritable, Directions>> edges) {
    this.edges = edges;
    lookAhead();
    reusable = new IntWritable();
  }

  @Override
  public boolean hasNext() {
    return currentTargetVertex != null;
  }

  @Override
  public IntWritable next() {
    reusable.set(currentTargetVertex.get());
    lookAhead();
    return reusable;
  }

  private void lookAhead() {
    currentTargetVertex = null;
    while (edges.hasNext()) {
      Edge<IntWritable, Directions> nextEdge = edges.next();
      if (nextEdge.getValue().isIncident()) {
        currentTargetVertex = nextEdge.getTargetVertexId();
        break;
      }
    }
  }
}
