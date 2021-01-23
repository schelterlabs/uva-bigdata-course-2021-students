# Assignment 1 [25 points]

Please implement the code for the following tasks and submit a zip file via Canvas. Make sure to write your code in a way that it would work in a real distributed execution in Hadoop. You can use the unit tests as a means to debug and validate your implementation. Note that a successful unit test execution does not necessary mean that your solution is 100% correct.

## Task 0: Code Submission [5 points]

Make sure that your submitted code compiles.

## Task 1: Serialization [2 points]

The class [BooksWritable](BooksWritable.java) manages an array of [Book](Book.java) objects during distributed processing. Implement the methods required for serializing this class.

_You can test your implementation with the following unit test:_
```
mvn -Dtest=nl.uva.bigdata.hadoop.assignment1.BooksWritableTest test
```

## Task 2: Counting words with MapReduce [4 + 2 points]

Implement distributed word counting in Hadoop via the [FilteringWordCount](FilteringWordCount.java) class. Count the number of occurrences of each lowercased word, but filter out the following terms first: "to", "the". Your job should produce a textual output with a word and its corresponding count (separated by the tab character) per line.

_You can test your implementation with the following unit tests:_
```
mvn -Dtest=nl.uva.bigdata.hadoop.assignment1.FilteringWordCountLocalTest test
mvn -Dtest=nl.uva.bigdata.hadoop.assignment1.FilteringWordCountClusterTest test
```

## Task 2: Aggregating measurements with MapReduce [4 + 2 points]

Your next task is to implement a MapReduceJob in the class [AverageTemperaturePerMonth](AverageTemperaturePerMonth.java), which aggregates sensor measurements. The input to this task are a collection of measurements, comprised of the year, month, temperature, and the quality. Your task is to compute the mean temperature per year and month for all measurements that are greater or equal to a specified quality threshold. Your job should produce a textual output with a year, a month and its corresponding mean temperature (separated by the tab character) per line.

_You can test your implementation with the following unit tests:_
```
mvn -Dtest=nl.uva.bigdata.hadoop.assignment1.AverageTemperaturePerMonthLocalTest test
mvn -Dtest=nl.uva.bigdata.hadoop.assignment1.AverageTemperaturePerMonthClusterTest test
```


## Task 4: Joins with MapReduce [4 + 2 points]

The final task is to implement a join in MapReduce in the class [BookAndAuthorReduceSideJoin](BookAndAuthorReduceSideJoin.java). The join will be executed on a set of books and authors with a foreign key relation that denotes the author for each book. Please implement a "reduce-side join" that repartitions both inputs based on the join key and executes the local join in the reducer. Your job should produce a textual output with the author name, the book name and the year of the book (separated by the tab character) per line.

_You can test your implementation with the following unit tests:_
```
mvn -Dtest=nl.uva.bigdata.hadoop.assignment1.BookAndAuthorReduceSideJoinLocalTest test
mvn -Dtest=nl.uva.bigdata.hadoop.assignment1.BookAndAuthorReduceSideJoinClusterTest test
```
