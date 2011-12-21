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

package org.apache.giraph.graph;

import com.google.common.collect.UnmodifiableIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.mahout.math.function.IntProcedure;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.set.OpenIntHashSet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class IntIntNullIntVertex extends
        BasicVertex<IntWritable, IntWritable, NullWritable,IntWritable> {

    private int id;
    private int value;

    private OpenIntHashSet edges;
    private IntArrayList messages;

    @Override
    public void initialize(IntWritable vertexId, IntWritable vertexValue,
                           Map<IntWritable, NullWritable> edges, 
                           List<IntWritable> messages) {
        id = vertexId.get();
        value = vertexValue.get();
        this.edges = new OpenIntHashSet(edges.size());
        for (Map.Entry<IntWritable,NullWritable> entry : edges.entrySet()) {
            this.edges.add(entry.getKey().get());
        }
        this.messages = new IntArrayList(messages.size());
        for (IntWritable message : messages) {
            this.messages.add(message.get());
        }
    }

    @Override
    public IntWritable getVertexId() {
        return new IntWritable(id);
    }

    @Override
    public IntWritable getVertexValue() {
        return new IntWritable(value);
    }

    @Override
    public void setVertexValue(IntWritable vertexValue) {
        value = vertexValue.get();
    }

    @Override
    public Iterator<IntWritable> iterator() {
        return new UnmodifiablePartialIntArrayIterator(edges.keys().elements(),
                edges.size());
    }

    @Override
    public NullWritable getEdgeValue(IntWritable targetVertexId) {
        return NullWritable.get();
    }

    @Override
    public boolean hasEdge(IntWritable targetVertexId) {
        return edges.contains(targetVertexId.get());
    }

    @Override
    public int getNumOutEdges() {
        return edges.size();
    }

    @Override
    public void sendMsgToAllEdges(final IntWritable message) {
        edges.forEachKey(new IntProcedure() {
            @Override
            public boolean apply(int neighborID) {
                sendMsg(new IntWritable(neighborID), message);
                return true;
            }
        });
    }

    @Override
    public Iterable<IntWritable> getMessages() {
        return new Iterable<IntWritable>() {
            @Override
            public Iterator<IntWritable> iterator() {
                return new UnmodifiablePartialIntArrayIterator(
                        messages.elements(), messages.size());
            }
        };
    }

    @Override
    public void setMessages(Iterable<IntWritable> messages) {
        this.messages.clear();
        for (IntWritable message : messages) {
            this.messages.add(message.get());
        }
    }

    @Override
    void releaseResources() {
        messages.clear();
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeInt(value);
        out.writeInt(edges.size());
        IntArrayList keys = edges.keys();
        for (int n = 0; n < keys.size(); n++) {
            out.writeInt(keys.elements()[n]);
        }
        out.writeInt(messages.size());
        for (int n = 0; n < messages.size(); n++) {
            out.writeInt(messages.elements()[n]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        value = in.readInt();
        int numEdges = in.readInt();
        edges = new OpenIntHashSet(numEdges);
        for (int n = 0; n < numEdges; n++) {
            edges.add(in.readInt());
        }
        int numMessages = in.readInt();
        messages = new IntArrayList(numMessages);
        for (int n = 0; n < numMessages; n++) {
            messages.add(in.readInt());
        }
    }

    private class UnmodifiablePartialIntArrayIterator
            extends UnmodifiableIterator<IntWritable> {

        private final int[] arr;
        private final int length;
        private int offset;

        UnmodifiablePartialIntArrayIterator(int[] arr, int length) {
            this.arr = arr;
            this.length = length;
            offset = 0;
        }

        @Override
        public boolean hasNext() {
            return offset < length;
        }

        @Override
        public IntWritable next() {
            return new IntWritable(arr[offset++]);
        }
    }    
}
