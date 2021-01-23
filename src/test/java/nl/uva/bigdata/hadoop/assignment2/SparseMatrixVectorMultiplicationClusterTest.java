package nl.uva.bigdata.hadoop.assignment2;

import nl.uva.bigdata.hadoop.exercise2.DenseMatrixVectorMultiplication;
import nl.uva.bigdata.hadoop.exercise2.DenseMatrixVectorMultiplicationClusterTest;

public class SparseMatrixVectorMultiplicationClusterTest extends DenseMatrixVectorMultiplicationClusterTest {

    @Override
    public void test() throws Exception {
        multiplication(new SparseMatrixVectorMultiplication());
    }
}
