package org.apache.giraph.examples.hyperball;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;


public class HyperBall64TextInputFormat extends
    TextVertexInputFormat<LongWritable, ReachableVertices, NullWritable> {

  /** Separator of the vertex and neighbors */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new NeighborhoodFunctionEstimationVertexReader();
  }

  /** associated vertex reader */
  public class NeighborhoodFunctionEstimationVertexReader extends
      TextVertexReaderFromEachLineProcessed<String[]> {

    /** Cached vertex id for the current line */
    private LongWritable id = new LongWritable();

    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      id.set(Integer.parseInt(tokens[0]));
      return tokens;
    }

    @Override
    protected LongWritable getId(String[] tokens) throws IOException {
      return id;
    }

    @Override
    protected ReachableVertices getValue(String[] tokens) throws IOException {
      return new ReachableVertices();
    }

    @Override
    protected Iterable<Edge<LongWritable, NullWritable>> getEdges(
        String[] tokens) throws IOException {
      List<Edge<LongWritable, NullWritable>> edges =
          Lists.newArrayListWithCapacity(tokens.length - 1);
      for (int n = 1; n < tokens.length; n++) {
        edges.add(EdgeFactory.create(
            new LongWritable(Long.parseLong(tokens[n]))));
      }
      return edges;
    }
  }
}
