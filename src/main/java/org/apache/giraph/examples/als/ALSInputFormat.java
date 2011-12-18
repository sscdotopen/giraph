package org.apache.giraph.examples.als;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.SortedMap;
import java.util.regex.Pattern;

public class ALSInputFormat
        extends TextVertexInputFormat<RowOrColumn,FeatureVector,IntWritable,
        FeatureVectorFrom> {
    @Override
    public VertexReader createVertexReader(InputSplit split, 
            TaskAttemptContext context) throws IOException {
        return new MatrixVertexReader(textInputFormat.createRecordReader(split,
                context));
    }
    
    static class MatrixVertexReader 
            extends TextVertexReader<RowOrColumn,FeatureVector,IntWritable,
            FeatureVectorFrom> {

        private static final Pattern SEPARATOR = Pattern.compile(" ");
        private static final Pattern SEPARATOR2 = Pattern.compile(",");

        public MatrixVertexReader(RecordReader<LongWritable,Text> reader) {
            super(reader);
        }

        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }

        @Override
        public BasicVertex<RowOrColumn,FeatureVector,IntWritable,
                FeatureVectorFrom> getCurrentVertex() throws IOException,
                InterruptedException {
            Text line = getRecordReader().getCurrentValue();
            String[] tokens = SEPARATOR.split(line.toString());
            BasicVertex<RowOrColumn,FeatureVector,IntWritable,FeatureVectorFrom>
                    vertex = BspUtils.<RowOrColumn,FeatureVector,
                    IntWritable, FeatureVectorFrom>createVertex(getContext().
                    getConfiguration());
            
            boolean isRow = "-".equals(tokens[0]);
            int index = Integer.parseInt(tokens[1]);

            SortedMap<RowOrColumn,IntWritable> edges = Maps.newTreeMap();

            for (int n = 2; n < tokens.length; n++) {
                String[] parts = SEPARATOR2.split(tokens[n]);
                edges.put(new RowOrColumn(!isRow, Integer.parseInt(parts[0])),
                        new IntWritable(Integer.parseInt(parts[1])));
            }

            vertex.initialize(new RowOrColumn(isRow, index),
                    new FeatureVector(), edges,
                    Lists.<FeatureVectorFrom>newArrayList());
            return vertex;
        }
    }
}
