package nl.uva.bigdata.hadoop.exercise1;


import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import nl.uva.bigdata.hadoop.HadoopLocalTestCase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class WordCountLocalTest extends HadoopLocalTestCase {

    @Test
    public void countWords() throws Exception {

        File inputFile = getTestTempFile("lotr.txt");
        File outputDir = getTestTempDir("output");
        outputDir.delete();

        writeLines(inputFile,
                "One ring to rule them all",
                "One ring to find them",
                "One ring to bring them all",
                "and in the darkness bind them");

        WordCount wordCount = new WordCount();

        wordCount.runLocal(
                new String[] { "--input", inputFile.getAbsolutePath(), "--output", outputDir.getAbsolutePath() });

        Map<String, Integer> counts = getCounts(new File(outputDir, "part-r-00000"));

        assertEquals(new Integer(3), counts.get("ring"));
        assertEquals(new Integer(2), counts.get("all"));
        assertEquals(new Integer(1), counts.get("darkness"));
    }

    protected Map<String,Integer> getCounts(File outputFile) throws IOException {
        Map<String,Integer> counts = Maps.newHashMap();
        for (String line: Files.readLines(outputFile, Charsets.UTF_8)) {
            String[] tokens = line.split("\t");
            counts.put(tokens[0], Integer.parseInt(tokens[1]));
        }
        return counts;
    }
}
