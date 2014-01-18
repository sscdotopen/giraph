package org.apache.giraph.examples.closeness;


import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class ClosenessTextInputFormat extends
  TextVertexInputFormat<IntWritable, ClosenessState, NullWritable> {

  /** Separator of the vertex and neighbors */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public TextVertexInputFormat.TextVertexReader createVertexReader(
      InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new ClosenessVertexReader();
  }

  /** vertex reader associated with {@link ClosenessTextInputFormat} */
  public class ClosenessVertexReader extends
      TextVertexReaderFromEachLineProcessed<String[]> {

    /** Cached vertex id for the current line */
    private IntWritable id;

    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      id = new IntWritable(Integer.parseInt(tokens[0]));
      return tokens;
    }

    @Override
    protected IntWritable getId(String[] tokens) throws IOException {
      return id;
    }

    @Override
    protected ClosenessState getValue(String[] tokens) throws IOException {
      return new ClosenessState();
    }

    @Override
    protected Iterable<Edge<IntWritable, NullWritable>> getEdges(
        String[] tokens) throws IOException {
      List<Edge<IntWritable, NullWritable>> edges =
          Lists.newArrayListWithCapacity(tokens.length - 1);
      for (int n = 1; n < tokens.length; n++) {
        edges.add(EdgeFactory.create(
            new IntWritable(Integer.parseInt(tokens[n]))));
      }
      return edges;
    }
  }
}
