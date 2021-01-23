# Exercise 2

## Task: Distributed Matrix Vector Multiplication in MapReduce

Implement a dense vector backed by an array in the class [DenseVector](DenseVector.java), and the ability to compute dot products with another dense vector. 

Next, please implement a distributed matrix vector multiplication via a broadcast-join in the class [DenseMatrixVectorMultiplication](DenseMatrixVectorMultiplication.java). We read the matrix row-wise, broadcast the vector to all workers and compute the multiplication results in a mapper.


_You can test your implementation with the following unit tests:_
```
mvn -Dtest=nl.uva.bigdata.hadoop.assignment2.SparseMatrixVectorMultiplicationLocalTest test
mvn -Dtest=nl.uva.bigdata.hadoop.assignment2.SparseMatrixVectorMultiplicationClusterTest test
```
