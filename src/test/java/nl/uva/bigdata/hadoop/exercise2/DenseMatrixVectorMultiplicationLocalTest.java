package nl.uva.bigdata.hadoop.exercise2;

import org.junit.Test;

public class DenseMatrixVectorMultiplicationLocalTest extends MatrixVectorMultiplicationLocalTest {

    @Test
    public void denseMultiplication() throws Exception {
        multiplication(new DenseMatrixVectorMultiplication());
    }
}
