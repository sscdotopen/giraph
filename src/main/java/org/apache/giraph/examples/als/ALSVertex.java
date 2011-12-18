package org.apache.giraph.examples.als;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.als.AlternateLeastSquaresSolver;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ALSVertex extends Vertex<RowOrColumn,FeatureVector,IntWritable,
            FeatureVectorFrom> {

    public static final String NUM_FEATURES = ALSVertex.class.getName() +
            ".numFeatures";
    public static final String NUM_ITERATIONS = ALSVertex.class.getName() +
            ".numIterations";
    public static final String LAMBDA = ALSVertex.class.getName() +
            ".lambda";

    private final AlternateLeastSquaresSolver solver =
            new AlternateLeastSquaresSolver();

    private int numFeatures() {
        return Integer.parseInt(getContext().getConfiguration()
                .get(NUM_FEATURES));
    }

    private int numIterations() {
        return Integer.parseInt(getContext().getConfiguration()
                .get(NUM_ITERATIONS));
    }

    private double lambda() {
        return Double.parseDouble(getContext().getConfiguration()
                .get(LAMBDA));
    }


    @Override
    public void compute(Iterator<FeatureVectorFrom> messages)
            throws IOException {


        if (getSuperstep() == 0) {
            if (!getVertexId().isRow()) {
                System.out.println("Initializing " + getVertexId());
                double[] features = initializeFeatureVector();
                setVertexValue(new FeatureVector(features));
                sendMsgToAllEdges(new FeatureVectorFrom(getVertexId().index(),
                            getVertexValue()));
            }
            return;
        }

        if (getSuperstep() >= 2 * numIterations()) {
            voteToHalt();
            return;
        }

        if ((getVertexId().isRow() && getSuperstep() % 2 == 1) ||
                (!getVertexId().isRow() && getSuperstep() % 2 == 0)) {

            Map<Integer,double[]> featuresFromNeighbors =
                    Maps.newHashMapWithExpectedSize(getNumOutEdges());

            while (messages.hasNext()) {
                FeatureVectorFrom featureVectorFrom = messages.next();
                featuresFromNeighbors.put(featureVectorFrom.from(),
                        featureVectorFrom.featureVector());
            }

            List<Vector> featureVectors =
                    Lists.newArrayListWithExpectedSize(getNumOutEdges());
            Vector ratings = new DenseVector(getNumOutEdges());
            int n = 0;
            Iterator<RowOrColumn> neighbors = iterator();
            while (neighbors.hasNext()) {
                RowOrColumn neighbor = neighbors.next();
                ratings.setQuick(n, getEdgeValue(neighbor).get());
                featureVectors.add(new DenseVector(featuresFromNeighbors
                        .get(neighbor.index())));
                n++;
            }

            Vector newFeatureVector = solver.solve(featureVectors, ratings,
                    lambda(), numFeatures());
            setVertexValue(new FeatureVector(asArray(newFeatureVector)));
            sendMsgToAllEdges(new FeatureVectorFrom(getVertexId().index(),
                        getVertexValue()));
        }


    }

    private double[] asArray(Vector newFeatureVector) {
        double[] arr = new double[numFeatures()];
        for (int n = 0; n < numFeatures(); n++) {
            arr[n] = newFeatureVector.get(n);
        }
        return arr;
    }

    private double[] initializeFeatureVector() {
        int ratingSum = 0;
        Iterator<RowOrColumn> neighbors = iterator();
        while (neighbors.hasNext()) {
            ratingSum += getEdgeValue(neighbors.next()).get();
        }
        double averageRating = (double)ratingSum / (double)getNumOutEdges();
        Random random = RandomUtils.getRandom();
        double[] features = new double[numFeatures()];
        features[0] = averageRating;
        for (int n = 1; n < numFeatures(); n++) {
            features[n] = random.nextDouble();
        }
        return features;
    }
}
