# Exercise 1

## Task 1: Counting words with MapReduce

Counting words in a distributed manner is the "hello world" application of Big Data systems. Have a look at the [WordCount](WordCount.java) class and run it in both local and cluster mode with the following unit tests.
```
mvn -Dtest=nl.uva.bigdata.hadoop.exercise1.WordCountLocalTest test
mvn -Dtest=nl.uva.bigdata.hadoop.exercise1.WordCountClusterTest test
```

## Task 2: Serialization

Hadoop (and all other Big Data systems) must be able to serialize data (convert it to a byte representation) in order to transmit it over the network and store it on disk. 
Hadoop requires you to implement the so-called _Writable_ interface in order to convert a byte representation to bytes and restore it from a byte representation. 
Implement the corresponding methods in the [NumberPairWritable](NumberPairWritable.java) class.

_You can test your implementation with the following unit test:_
```
mvn -Dtest=nl.uva.bigdata.hadoop.exercise1.NumberPairWritableTest test
```

## Task 3: Document Indexing with MapReduce

The main reason why MapReduce was designed has been to build indexes of the web at scale. You will now write your first MapReduce job to do something similar. Given a collection of [documents representing web pages](../../../../../../../test/resources/websites.tsv) with an id, their url and text, we want to create an invertex index that points from words to the page ids. Your MapReduce job should output a lower-cased word, a page id and the number of total pages in which the word occurs for each occurrence of a word.

_You can test your implementation with the following unit tests:_
```
mvn -Dtest=nl.uva.bigdata.hadoop.exercise1.InvertedIndexLocalTest test
mvn -Dtest=nl.uva.bigdata.hadoop.exercise1.InvertedIndexClusterTest test
```

## Task 4: Joins with MapReduce

Next, we will implement a join in MapReduce to join together a collection of [books](../../../../../../../test/resources/books.tsv) and authors [authors](../../../../../../../test/resources/authors.tsv) based on a foreign-key relationship. MapReduce does not have operators for multiple inputs, which makes this task rather difficult. We implement a so-called map-side broadcast join here, where we use Hadoop's _DistributedCache_ to broadcast the smaller dataset to all workers and execute the join in the map function. Your job should produce a textual output with the author name, the book name and the year of the book (separated by the tab character) per line.

_You can test your implementation with the following unit tests:_
```
mvn -Dtest=nl.uva.bigdata.hadoop.exercise1.BookAndAuthorMapSideJoinLocalTest test
mvn -Dtest=nl.uva.bigdata.hadoop.exercise1.BookAndAuthorMapSideJoinClusterTest test
```
