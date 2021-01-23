package nl.uva.bigdata.hadoop.assignment2;

import org.apache.commons.math3.linear.RealMatrix;

import java.util.Collection;

class Tuple<T1, T2> {
    T1 first;
    T2 second;

    public Tuple(T1 first, T2 second) {
        this.first = first;
        this.second = second;
    }
}

public class DistributedLinearRegression {

    static class OuterProducts implements
            MapFunction<Integer, Tuple<double[], Double>, Integer, Tuple<RealMatrix, RealMatrix>> {

        @Override
        public Collection<Record<Integer, Tuple<RealMatrix, RealMatrix>>> map(
                Record<Integer, Tuple<double[], Double>> rowOfXwithy
        ) {
            //TODO Implement me
            throw new IllegalStateException("Not implemented");
        }
    }

    static class Solver implements ReduceFunction<Integer, Tuple<RealMatrix, RealMatrix>, RealMatrix> {

        @Override
        public Collection<Record<Integer, RealMatrix>> reduce(
                Integer key, Collection<Tuple<RealMatrix, RealMatrix>> valueGroup
        ) {
            //TODO Implement me
            throw new IllegalStateException("Not implemented");
        }
    }



}
