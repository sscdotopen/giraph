package org.apache.giraph.examples;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

public class SymmetricGraphTextInputFormat extends
        TextVertexInputFormat<IntWritable,IntWritable,NullWritable,
                    IntWritable> {
    @Override
    public VertexReader<IntWritable,IntWritable,NullWritable,IntWritable>
    createVertexReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        return new HCCVertexReader(
                textInputFormat.createRecordReader(split, context));
    }

    public static class HCCVertexReader extends
            TextVertexInputFormat.TextVertexReader<IntWritable,IntWritable,
                    NullWritable,IntWritable> {

        private static final Pattern SEPARATOR = Pattern.compile(" ");

        public HCCVertexReader(RecordReader<LongWritable, Text> lineReader) {
            super(lineReader);
        }

        @Override
        public BasicVertex<IntWritable,IntWritable,NullWritable,IntWritable>
        getCurrentVertex()
                throws IOException, InterruptedException {
            BasicVertex<IntWritable,IntWritable,NullWritable,IntWritable> vertex
                    = BspUtils.<IntWritable,IntWritable,NullWritable,
                    IntWritable>createVertex(getContext().getConfiguration());

            String[] tokens = SEPARATOR.split(getRecordReader()
                    .getCurrentValue().toString());
            Map<IntWritable,NullWritable> edges =
                    Maps.newHashMapWithExpectedSize(tokens.length - 1);
            for (int n = 1; n < tokens.length; n++) {
                edges.put(new IntWritable(Integer.parseInt(tokens[n])),
                        NullWritable.get());
            }

            IntWritable vertexId = new IntWritable(Integer.parseInt(tokens[0]));
            vertex.initialize(vertexId, vertexId, edges,
                    Lists.<IntWritable>newArrayList());

            return vertex;
        }

        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }
    }

}
