package org.apache.giraph.examples.als;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FeatureVectorFrom implements Writable {

    private int index;
    private FeatureVector featureVector;

    public FeatureVectorFrom() {
    }

    public FeatureVectorFrom(int index, FeatureVector featureVector) {
        this.index = index;
        this.featureVector = featureVector;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(index);
        featureVector.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        index = in.readInt();
        featureVector = new FeatureVector();
        featureVector.readFields(in);
    }

    public double[] featureVector() {
        return featureVector.get();
    }

    public int from() {
        return index;
    }
}
