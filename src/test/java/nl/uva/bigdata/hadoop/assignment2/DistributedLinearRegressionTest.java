package nl.uva.bigdata.hadoop.assignment2;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DistributedLinearRegressionTest {

    @Test
    public void solve() {

        double[][] valuesOfX = {
            { 2, 2, 10.5, 10 },
            { 1, 2, 12,   12 },
            { 1, 1, 12,   13 },
            { 2, 1, 11,   13 },
            { 1, 2, 12,   11 },
            { 2, 1, 16,   8 },
            { 6, 2, 17,   1 },
            { 3, 2, 13,   7 },
            { 3, 3, 13,   4 }
        };

        double[] valuesOfy = {
            29.509541,
            18.042851,
            22.736446,
            32.207582,
            21.871292,
            36.187559,
            50.764999,
            40.400208,
            45.811716
        };

        RealMatrix X = new Array2DRowRealMatrix(valuesOfX);
        RealMatrix y = new Array2DRowRealMatrix(valuesOfy);

        RealMatrix XtX = X.transpose().multiply(X);
        RealMatrix Xty = X.transpose().multiply(y);

        RealMatrix betaHat = new LUDecomposition(XtX).getSolver().solve(Xty);

        List<Record<Integer, Tuple<double[], Double>>> inputs = new ArrayList<>();

        for (int n = 0; n < X.getRowDimension(); n++) {
            inputs.add(new Record<>(n, new Tuple<>(X.getRow(n), y.getEntry(n, 0))));
        }

        MapReduceEngine<Integer, Tuple<double[], Double>, Integer, Tuple<RealMatrix, RealMatrix>, RealMatrix> engine =
            new MapReduceEngine<>();

        Collection<Record<Integer, RealMatrix>> mapReduceResults = engine.compute(
                inputs,
                new DistributedLinearRegression.OuterProducts(),
                new DistributedLinearRegression.Solver(),
                2
        );

        RealMatrix betaHatMapReduce = mapReduceResults.iterator().next().getValue();

        RealMatrix yHat = X.multiply(betaHat);
        double rmse = Math.sqrt((yHat.subtract(y)).getNorm());
        RealMatrix yHatMapReduce = X.multiply(betaHatMapReduce);
        double rmseMapReduce = Math.sqrt((yHatMapReduce.subtract(y)).getNorm());

        assertEquals(rmse, rmseMapReduce, 0.00001);
    }

}
