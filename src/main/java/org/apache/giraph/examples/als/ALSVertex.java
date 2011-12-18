package org.apache.giraph.examples.als;

import com.google.common.collect.Lists;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.als.AlternateLeastSquaresSolver;
import org.apache.mahout.math.map.OpenIntObjectHashMap;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * BSP implementation of "Large-scale Parallel Collaborative Filtering for the Netflix Prize" available at
 * http://www.hpl.hp.com/personal/Robert_Schreiber/papers/2008%20AAIM%20Netflix/netflix_aaim08(submitted).pdf.</p>
 */
public class ALSVertex extends Vertex<RowOrColumn,FeatureVector,IntWritable,
            FeatureVectorFrom> {

    public static final String NUM_FEATURES = ALSVertex.class.getName() +
            ".numFeatures";
    public static final String NUM_ITERATIONS = ALSVertex.class.getName() +
            ".numIterations";
    public static final String LAMBDA = ALSVertex.class.getName() +
            ".lambda";

    private static final AlternateLeastSquaresSolver SOLVER =
            new AlternateLeastSquaresSolver();

    @Override
    public void compute(Iterator<FeatureVectorFrom> messages)
            throws IOException {

        /* initialize row vertices in first iteration */
        if (getSuperstep() == 0) {
            if (!getVertexId().isRow()) {
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

            OpenIntObjectHashMap<double[]> featuresFromNeighbors =
                collectFeaturesFromNeighbors();

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

            setVertexValue(recompute(featureVectors, ratings));
            sendMsgToAllEdges(new FeatureVectorFrom(getVertexId().index(),
                    getVertexValue()));
        }
    }

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
    
    private FeatureVector recompute(List<Vector> featureVectors,
            Vector ratings) {
        Vector newFeatureVector = SOLVER.solve(featureVectors, ratings,
                lambda(), numFeatures());
        double[] newFeatures = new double[numFeatures()];
        for (int n = 0; n < numFeatures(); n++) {
            newFeatures[n] = newFeatureVector.get(n);
        }
        return new FeatureVector(newFeatures);
    }

    private OpenIntObjectHashMap<double[]> collectFeaturesFromNeighbors() {
        OpenIntObjectHashMap<double[]> featuresFromNeighbors =
                new OpenIntObjectHashMap<double[]>();

        for (FeatureVectorFrom featureVectorFrom : getMessages()) {
            featuresFromNeighbors.put(featureVectorFrom.from(),
                    featureVectorFrom.featureVector());
        }
        return featuresFromNeighbors;
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
