package nl.uva.bigdata.hadoop.assignment2;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MapReduceEngineTest {

    static class Tokenize implements MapFunction<Integer, String, String, Integer> {

        @Override
        public Collection<Record<String, Integer>> map(Record<Integer, String> input) {
            List<Record<String, Integer>> outputs = new ArrayList<>();
            StringTokenizer tokenizer = new StringTokenizer(input.getValue());
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken().toLowerCase();
                outputs.add(new Record<>(word, 1));
            }

            return outputs;
        }
    }

    static class Sum implements ReduceFunction<String, Integer, Integer> {

        @Override
        public Collection<Record<String, Integer>> reduce(String word, Collection<Integer> valueGroup) {
            List<Record<String, Integer>> outputs = new ArrayList<>();

            int sum = 0;
            for (Integer value : valueGroup) {
                sum += value;
            }

            outputs.add(new Record<>(word, sum));

            return outputs;
        }
    }


    @Test
    public void wordCount() {

        List<Record<Integer, String>> inputs = new ArrayList<>();
        inputs.add(new Record<>(1, "Hello World"));
        inputs.add(new Record<>(2, "Hello Universe"));
        inputs.add(new Record<>(3, "Hello Everyone"));

        MapReduceEngine<Integer, String, String, Integer, Integer> engine = new MapReduceEngine<>();

        Collection<Record<String, Integer>> results =
                engine.compute(inputs, new Tokenize(), new Sum(), 2);

        assertEquals(4, results.size());

        Map<String, Integer> resultsByKey = new HashMap<>();

        for (Record<String, Integer> record : results) {
            resultsByKey.put(record.getKey(), record.getValue());
        }

        assertEquals(4, resultsByKey.size());

        assertTrue(resultsByKey.containsKey("hello"));
        assertEquals(3, resultsByKey.get("hello").intValue());

        assertTrue(resultsByKey.containsKey("world"));
        assertEquals(1, resultsByKey.get("world").intValue());
    }

}
