package org.apache.giraph.examples.als;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class FeatureVector implements Writable {

    private double[] features;

    public FeatureVector() {
        features = new double[0];
    }

    public FeatureVector(double[] features) {
        this.features = features;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(features.length);
        for (int n = 0; n < features.length; n++) {
            out.writeDouble(features[n]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        features = new double[in.readInt()];
        for (int n = 0; n < features.length; n++) {
            features[n] = in.readDouble();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof FeatureVector) {
            return Arrays.equals(features, ((FeatureVector) o).features);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(features);
    }

    public double[] get() {
        return features;
    }
}