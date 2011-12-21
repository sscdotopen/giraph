package org.apache.giraph.examples;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class HCCVertexOutputFormat extends
        TextVertexOutputFormat<IntWritable, IntWritable, NullWritable> {

    @Override
    public VertexWriter<IntWritable, IntWritable, NullWritable>
    createVertexWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        RecordWriter<Text, Text> recordWriter =
                textOutputFormat.getRecordWriter(context);
        return new HCCVertexWriter(recordWriter);
    }

    public static class HCCVertexWriter extends
            TextVertexOutputFormat.TextVertexWriter<IntWritable, IntWritable,
                    NullWritable> {

        public HCCVertexWriter(RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(BasicVertex<IntWritable, IntWritable,
                NullWritable,?> vertex) throws IOException,
                InterruptedException {
            StringBuilder output = new StringBuilder();
            output.append(vertex.getVertexId().get());
            output.append(" ");
            output.append(vertex.getVertexValue().get());
            getRecordWriter().write(new Text(output.toString()), null);
        }

    }
}