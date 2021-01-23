package nl.uva.bigdata.hadoop.exercise1;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import nl.uva.bigdata.hadoop.HadoopClusterTestCase;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Pattern;


public class InvertedIndexClusterTest extends HadoopClusterTestCase {

    public void test() throws Exception {

        Path websitesFile = path("websites.tsv");

        writeLines(websitesFile, readResource("/websites.tsv"));
        Path outputDir = path("output");

        InvertedIndex index = new InvertedIndex();

        index.runOnCluster(createJobConf(),
                new String[] { "--input", websitesFile.toString(), "--output", outputDir.toString() });

        Multimap<String, Pair<Integer, Integer>> results = readResults(new Path(outputDir, "part-r-00000"));

        assertTrue(results.containsKey("apache"));

        Collection<Pair<Integer, Integer>> apacheResults = results.get("apache");
        assertEquals(apacheResults.size(), 3);

        for (Pair<Integer, Integer> pageOccurrence : apacheResults) {
            assertEquals(pageOccurrence.getSecond(), new Integer(3));
        }

        assertTrue(results.containsKey("flink"));

        Collection<Pair<Integer, Integer>> flinkResults = results.get("flink");
        assertEquals(flinkResults.size(), 1);

        Pair<Integer, Integer> flinkOccurrence = new ArrayList<>(flinkResults).get(0);
        assertEquals(flinkOccurrence.getFirst(), new Integer(3));
        assertEquals(flinkOccurrence.getSecond(), new Integer(1));
    }

    private Multimap<String, Pair<Integer, Integer>> readResults(Path outputFile) throws IOException {
        Pattern separator = Pattern.compile("\t");

        Multimap<String, Pair<Integer, Integer>> results = HashMultimap.create();

        for (String line : readLines(outputFile)) {
            String[] tokens = separator.split(line);
            String word = tokens[0];
            int pageId = Integer.parseInt(tokens[1]);
            int numOccurrences = Integer.parseInt(tokens[2]);

            results.put(word, new Pair<>(pageId, numOccurrences));
        }

        return results;
    }

}
