package nl.uva.bigdata.hadoop.assignment2;

import nl.uva.bigdata.hadoop.exercise2.MatrixVectorMultiplicationLocalTest;
import org.junit.Test;

public class SparseMatrixVectorMultiplicationLocalTest extends MatrixVectorMultiplicationLocalTest {

    @Test
    public void sparseMultiplication() throws Exception {
        multiplication(new SparseMatrixVectorMultiplication());
    }
}
