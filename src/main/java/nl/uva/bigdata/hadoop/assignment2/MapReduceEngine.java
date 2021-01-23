package nl.uva.bigdata.hadoop.assignment2;


import java.util.*;

class Record<K  extends Comparable<K>, V> {
    K key;
    V value;

    public Record(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }
}

interface MapFunction<K1 extends Comparable<K1>, V1, K2 extends Comparable<K2>, V2> {
    Collection<Record<K2, V2>> map(Record<K1, V1> inputRecord);
}

interface ReduceFunction<K2 extends Comparable<K2>, V2, V3> {
    Collection<Record<K2, V3>> reduce(K2 key, Collection<V2> valueGroup);
}

public class MapReduceEngine<K1 extends Comparable<K1>, V1, K2 extends Comparable<K2>, V2, V3> {

    private Collection<Record<K2, V2>> runMapPhase(
            Collection<Record<K1, V1>> inputRecords,
            MapFunction<K1, V1, K2, V2> map
    ) {
        //TODO Implement me
        throw new IllegalStateException("Not implemented");
    }

    private Collection<Collection<Record<K2, V2>>> partitionMapOutputs(
            Collection<Record<K2, V2>> mapOutputs,
            int numPartitions) {

        //TODO Implement me
        throw new IllegalStateException("Not implemented");
    }

    private Map<K2, Collection<V2>> groupReducerInputPartition(Collection<Record<K2, V2>> reducerInputPartition) {

        //TODO Implement me
        throw new IllegalStateException("Not implemented");
    }

    private Collection<Record<K2, V3>> runReducePhaseOnPartition(
            Map<K2, Collection<V2>> reducerInputs,
            ReduceFunction<K2, V2, V3> reduce
    ) {
        //TODO Implement me
        throw new IllegalStateException("Not implemented");
    }

    public Collection<Record<K2, V3>> compute(
        Collection<Record<K1, V1>> inputRecords,
        MapFunction<K1, V1, K2, V2> map,
        ReduceFunction<K2, V2, V3> reduce,
        int numPartitionsDuringShuffle
    ) {

        Collection<Record<K2, V2>> mapOutputs = runMapPhase(inputRecords, map);
        Collection<Collection<Record<K2, V2>>> partitionedMapOutput =
                partitionMapOutputs(mapOutputs, numPartitionsDuringShuffle);

        assert numPartitionsDuringShuffle == partitionedMapOutput.size();

        List<Record<K2, V3>> outputs = new ArrayList<>();

        for (Collection<Record<K2, V2>> partition : partitionedMapOutput) {
            Map<K2, Collection<V2>> reducerInputs = groupReducerInputPartition(partition);

            outputs.addAll(runReducePhaseOnPartition(reducerInputs, reduce));
        }


        return outputs;
    }

}
