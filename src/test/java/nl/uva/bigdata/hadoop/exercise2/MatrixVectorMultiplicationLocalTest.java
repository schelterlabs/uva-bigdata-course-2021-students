package nl.uva.bigdata.hadoop.exercise2;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import nl.uva.bigdata.hadoop.HadoopLocalTestCase;
import nl.uva.bigdata.hadoop.HadoopJob;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class MatrixVectorMultiplicationLocalTest extends HadoopLocalTestCase {

    protected void multiplication(HadoopJob multiplication) throws Exception {
        File vectorFile = getTestTempFile("vector.tsv");
        File matrixFile = getTestTempFile("matrix.tsv");
        File outputDir = getTestTempDir("output");
        outputDir.delete();

        writeLines(vectorFile, readLines("/vector.tsv"));
        writeLines(matrixFile, readLines("/matrix.tsv"));


        multiplication.runLocal(new String[]{"--matrix", matrixFile.getAbsolutePath(),
                "--vector", vectorFile.getAbsolutePath(), "--output", outputDir.getAbsolutePath()});

        Map<Integer, Double> results = readResults(new File(outputDir, "part-m-00000"));

        double delta = 0.000001;

        assertEquals(3, results.size());
        assertEquals(17.0, results.get(0), delta);
        assertEquals(1.0, results.get(1), delta);
        assertEquals(3.0, results.get(2), delta);
    }

    Map<Integer, Double> readResults(File outputFile) throws IOException {
        Map<Integer, Double> results = new HashMap<>();

        Pattern separator = Pattern.compile("\t");
        for (String line : Files.readLines(outputFile, Charsets.UTF_8)) {
            String[] tokens = separator.split(line);
            results.put(Integer.parseInt(tokens[0]), Double.parseDouble(tokens[1]));
        }
        return results;
    }
}
