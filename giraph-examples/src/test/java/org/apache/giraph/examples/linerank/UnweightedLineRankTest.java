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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

public class UnweightedLineRankTest {

  @Test
  public void testToyData() throws Exception {

    String[] graph = new String[] {
        "1 2:true:true 3:true:true",
        "2 1:true:true 3:false:true",
        "3 1:true:true 2:true:false" };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(UnweightedLineRank.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);
    conf.setMessageCombinerClass(IntDoubleSumCombiner.class);
    conf.setVertexInputFormatClass(LineRankTextInputFormat.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    conf.enableOneToAllMsgSending();

    // run internally
    Iterable<String> results = InternalVertexRunner.run(conf, graph);

    for (String result : results) {
      System.out.println(result);
    }
  }
}
