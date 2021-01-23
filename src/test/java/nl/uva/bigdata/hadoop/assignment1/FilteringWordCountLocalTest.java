package nl.uva.bigdata.hadoop.assignment1;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import nl.uva.bigdata.hadoop.HadoopLocalTestCase;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class FilteringWordCountLocalTest extends HadoopLocalTestCase {

    @Test
    public void test() throws Exception {

        File inputFile = getTestTempFile("lotr.txt");
        File outputDir = getTestTempDir("outputs/");
        outputDir.delete();

        writeLines(inputFile,
                "One Ring to rule them all,",
                "One Ring to find them,",
                "One Ring to bring them all,",
                "and in the darkness bind them");

        FilteringWordCount wordCount = new FilteringWordCount();

        wordCount.runLocal(new String[] { "--input", inputFile.toString(), "--output", outputDir.toString() });

        Map<String, Integer> counts = getCounts(new File(outputDir, "part-r-00000"));

        assertEquals(new Integer(3), counts.get("ring"));
        assertEquals(new Integer(2), counts.get("all"));
        assertEquals(new Integer(1), counts.get("darkness"));
        assertFalse(counts.containsKey("the"));
        assertFalse(counts.containsKey("to"));
    }

    protected Map<String,Integer> getCounts(File outputFile) throws IOException {
        Map<String,Integer> counts = Maps.newHashMap();
        for (String line: Files.readLines(outputFile, StandardCharsets.UTF_8)) {
            String[] tokens = line.split("\t");
            counts.put(tokens[0], Integer.parseInt(tokens[1]));
        }
        return counts;
    }

}
