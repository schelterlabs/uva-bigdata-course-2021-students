package nl.uva.bigdata.hadoop.exercise2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DenseVector implements Vector, Writable {

    @Override
    public int dimension() {
        // TODO Implement me
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public double dot(Vector other) {

        // TODO Implement me
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Implement me
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Implement me
    }
}
