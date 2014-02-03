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


import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class LineRankTextInputFormat extends
  TextVertexInputFormat<IntWritable, DoubleWritable, Directions> {

  /** Separator of the vertex and neighbors */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  private static final Pattern SEPARATOR2 = Pattern.compile("[:]");

  @Override
  public TextVertexReader createVertexReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new LineRankVertexReader();
  }

  /** associated vertex reader */
  public class LineRankVertexReader extends
      TextVertexReaderFromEachLineProcessed<String[]> {

    /** Cached vertex id for the current line */
    private IntWritable id = new IntWritable();

    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      id.set(Integer.parseInt(tokens[0]));
      return tokens;
    }

    @Override
    protected IntWritable getId(String[] tokens) throws IOException {
      return id;
    }

    @Override
    protected DoubleWritable getValue(String[] tokens) throws IOException {
      return new DoubleWritable(0);
    }

    @Override
    protected Iterable<Edge<IntWritable, Directions>> getEdges(
        String[] tokens) throws IOException {
      List<Edge<IntWritable, Directions>> edges =
          Lists.newArrayListWithCapacity(tokens.length - 1);
      for (int n = 1; n < tokens.length; n++) {

        String[] parts = SEPARATOR2.split(tokens[n]);
        IntWritable target = new IntWritable(Integer.parseInt(parts[0]));
        Directions directions = new Directions(Boolean.parseBoolean(parts[1]),
            Boolean.parseBoolean(parts[2]));

        edges.add(EdgeFactory.create(target, directions));
      }
      return edges;
    }
  }
}
