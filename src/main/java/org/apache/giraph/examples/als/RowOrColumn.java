package org.apache.giraph.examples.als;


import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RowOrColumn implements WritableComparable<RowOrColumn> {

    private boolean isRow;
    private int index;

    public RowOrColumn() {
    }

    public RowOrColumn(boolean row, int index) {
        isRow = row;
        this.index = index;
    }

    public boolean isRow() {
        return isRow;
    }

    public int index() {
        return index;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(isRow);
        out.writeInt(index);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        isRow = in.readBoolean();
        index = in.readInt();
    }

    @Override
    public int compareTo(RowOrColumn other) {
        return ComparisonChain.start().compare(!isRow, !other.isRow)
                .compare(index, other.index).result();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof RowOrColumn) {
            RowOrColumn other = (RowOrColumn) o;
            return isRow == other.isRow && index == other.index;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return 31 * (isRow ? 1 : 0) + index;
    }

    @Override
    public String toString() {
        return  "[" + (isRow ? "row" : "column") + " " + index + "]";
    }
}
