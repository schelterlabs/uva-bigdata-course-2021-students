package nl.uva.bigdata.hadoop.exercise1;

import com.google.common.base.Charsets;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import nl.uva.bigdata.hadoop.HadoopLocalTestCase;
import org.apache.commons.math3.util.Pair;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InvertedIndexLocalTest extends HadoopLocalTestCase {

    @Test
    public void buildIndex() throws Exception {

        File websitesFile = getTestTempFile("websites.tsv");
        writeLines(websitesFile, readLines("/websites.tsv"));
        File outputDir = getTestTempDir("output");
        outputDir.delete();

        InvertedIndex index = new InvertedIndex();

        index.runLocal(
                new String[] { "--input", websitesFile.getAbsolutePath(), "--output", outputDir.getAbsolutePath() });

        Multimap<String, Pair<Integer, Integer>> results = readResults(new File(outputDir, "part-r-00000"));

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

    private Multimap<String, Pair<Integer, Integer>> readResults(File outputFile) throws IOException {
        Pattern separator = Pattern.compile("\t");

        Multimap<String, Pair<Integer, Integer>> results = HashMultimap.create();

        for (String line : Files.readLines(outputFile, Charsets.UTF_8)) {
            String[] tokens = separator.split(line);
            String word = tokens[0];
            int pageId = Integer.parseInt(tokens[1]);
            int numOccurrences = Integer.parseInt(tokens[2]);

            results.put(word, new Pair<>(pageId, numOccurrences));
        }

        return results;
    }

}
