package org.apache.giraph.examples.als;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import junit.framework.TestCase;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;

import java.util.List;
import java.util.Map;

public class ALSVertexTest extends TestCase {

    public void testIt() throws Exception {

        int numFeatures = 3;
        int numIterations = 10;
        double lambda = 0.065;

        double na = Double.NaN;
        Matrix ratings = new DenseMatrix(new double[][] {
                new double[] { 5.0, 5.0, 2.0, na },
                new double[] { 2.0, na,  3.0, 5.0 },
                new double[] { na,  5.0, na,  3.0 },
                new double[] { 3.0, na,  na,  5.0 } });

        List<String> graph = Lists.newArrayList();
        for (int m = 0; m < ratings.numRows(); m++) {
            StringBuilder line = new StringBuilder("- ");
            line.append(m);
            for (Vector.Element e : ratings.viewRow(m)) {
                if (!Double.isNaN(e.get())) {
                    line.append(" " + e.index() + "," + (int)e.get());
                }
            }
            graph.add(line.toString());
        }
        for (int n = 0; n < ratings.numCols(); n++) {
            StringBuilder line = new StringBuilder("| ");
            line.append(n);
            for (Vector.Element e : ratings.viewColumn(n)) {
                if (!Double.isNaN(e.get())) {
                    line.append(" " + e.index() + "," + (int)e.get());
                }
            }
            graph.add(line.toString());
        }

        Map<String,String> params = Maps.newHashMap();
        params.put(ALSVertex.NUM_FEATURES, String.valueOf(numFeatures));
        params.put(ALSVertex.NUM_ITERATIONS, String.valueOf(numIterations));
        params.put(ALSVertex.LAMBDA, String.valueOf(lambda));

        /* run internally */
        Iterable<String> results = InternalVertexRunner.run(
            ALSVertex.class, ALSInputFormat.class, ALSOutputFormat.class,
            params, graph.toArray(new String[]{}));

        Matrix userFeatures = new DenseMatrix(4, 3);
        Matrix itemFeatures = new DenseMatrix(4, 3);

        for (String result : results) {
            String[] tokens = result.split("\t");
            Matrix matrixToFill = "-".equals(tokens[0]) ? userFeatures :
                    itemFeatures;
            int index = Integer.parseInt(tokens[1]);
            for (int n = 0; n < 3; n++) {
                matrixToFill.set(index, n, Double.parseDouble(tokens[n + 2]));
            }
        }

        int knownRatings = 0;
        double squaredErrorSum = 0;
        for (int m = 0; m < ratings.numRows(); m++) {
            for (Vector.Element e : ratings.viewRow(m)) {
                if (!Double.isNaN(e.get())) {
                    double prediction = userFeatures.viewRow(m)
                            .dot(itemFeatures.viewRow(e.index()));
                    System.out.println("rating of user " + m + " for item " +
                            e.index()  + " was " + e.get() +
                            ", prediction is " + prediction);
                    double err = e.get() - prediction;
                    squaredErrorSum += err * err;
                    knownRatings++;
                }
            }
        }

        double rmse = Math.sqrt(squaredErrorSum / knownRatings);
        System.out.println("\n\nRMSE is " + rmse);
        assertTrue(rmse < 0.2);
    }

}
