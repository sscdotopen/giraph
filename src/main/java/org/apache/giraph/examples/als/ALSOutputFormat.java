package org.apache.giraph.examples.als;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class ALSOutputFormat
        extends TextVertexOutputFormat<RowOrColumn,FeatureVector,IntWritable> {
    @Override
    public VertexWriter<RowOrColumn, FeatureVector, IntWritable>
            createVertexWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        RecordWriter<Text, Text> recordWriter =
                textOutputFormat.getRecordWriter(context);
        return new ALSVertexWriter(recordWriter);
    }

    public static class ALSVertexWriter extends
            TextVertexWriter<RowOrColumn,FeatureVector,IntWritable> {

        public ALSVertexWriter(RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(BasicVertex<RowOrColumn, FeatureVector,
                IntWritable, ?> vertex) throws IOException,
                InterruptedException {
            String out = "";
            out += vertex.getVertexId().isRow() ? "-" : "|";
            out += "\t" + vertex.getVertexId().index();
            for (double value : vertex.getVertexValue().get()) {
                out += "\t" + value;
            }
            getRecordWriter().write(new Text(out), null);
        }
    }
}
