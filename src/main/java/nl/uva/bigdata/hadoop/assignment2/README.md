# Assignment 2 [20 points]

Please implement the code for the following tasks and submit a zip file via Canvas. Make sure to write your code in a way that it would work in a real distributed execution in Hadoop. You can use the unit tests as a means to debug and validate your implementation. Note that a successful unit test execution does not necessary mean that your solution is 100% correct.

## Task 0: Code Submission [5 points]

Make sure that your submitted code compiles.

## Task 1: Distributed Matrix Vector Multiplication in MapReduce [3 + 2 points]

Implement a sparse vector backed by a hashmap in the class [SparseVector](SparseVector.java). Next, please implement a distributed matrix vector multiplication via a broadcast-join in the class [SparseMatrixVectorMultiplication](SparseMatrixVectorMultiplication.java), analogous to the implementation from our exercises. Please use a dense representation for all vectors with a sparsity of less than 50% and a sparse representation otherwise.

_You can test your implementation with the following unit tests:_
```
mvn -Dtest=nl.uva.bigdata.hadoop.assignment2.SparseMatrixVectorMultiplicationLocalTest test
mvn -Dtest=nl.uva.bigdata.hadoop.assignment2.SparseMatrixVectorMultiplicationClusterTest test
```

## Task 2: Implement your own MapReduce engine [5 points]

In this task, we stop using Hadoop and implement our own (local) MapReduce engine in [MapReduceEngine.](MapReduceEngine.java). This reverses the previous tasks, now we are given the map and reduce implementations for word counting and we have to implement the underlying engine according to the three phases of execution in MapReduce.

_You can test your implementation with the following unit test:_
```
mvn -Dtest=nl.uva.bigdata.hadoop.assignment2.MapReduceEngineTest test
```

## Task 3: Implement Distributed Linear Regression [5 points]

Your final task is to implement distributed linear regression (as discussed in the class) on top of your own MapReduce engine in [DistributedLinearRegression](DistributedLinearRegression.java). Compute outer products in the mapper, sum up the intermediate results in the reducer and solve the corresponding linear system.

_You can test your implementation with the following unit test:_
```
mvn -Dtest=nl.uva.bigdata.hadoop.assignment2.DistributedLinearRegressionTest test
```
