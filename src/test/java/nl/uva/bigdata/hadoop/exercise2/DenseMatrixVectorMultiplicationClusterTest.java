package nl.uva.bigdata.hadoop.exercise2;

import nl.uva.bigdata.hadoop.HadoopClusterTestCase;
import nl.uva.bigdata.hadoop.HadoopJob;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;


public class DenseMatrixVectorMultiplicationClusterTest extends HadoopClusterTestCase {

        public void test() throws Exception {
            multiplication(new DenseMatrixVectorMultiplication());
        }

        protected void multiplication(HadoopJob multiplication) throws Exception {
            Path vectorFile = path("vector.tsv");
            Path matrixFile = path("matrix.tsv");
            Path outputDir = path("output");

            writeLines(vectorFile, readResource("/vector.tsv"));
            writeLines(matrixFile, readResource("/matrix.tsv"));

            multiplication.runOnCluster(createJobConf(),
                    new String[] { "--matrix", matrixFile.toString(), "--vector", vectorFile.toString(),
                            "--output", outputDir.toString() });

            Map<Integer, Double> results = readResults(new Path(outputDir, "part-m-00000"));

            double delta = 0.000001;

            assertEquals(3, results.size());
            assertEquals(17.0, results.get(0), delta);
            assertEquals(1.0, results.get(1), delta);
            assertEquals(3.0, results.get(2), delta);
        }

        Map<Integer, Double> readResults(Path outputFile) throws IOException {
            Map<Integer, Double> results = new HashMap<>();

            Pattern separator = Pattern.compile("\t");
            for (String line : readLines(outputFile)) {
                String[] tokens = separator.split(line);
                results.put(Integer.parseInt(tokens[0]), Double.parseDouble(tokens[1]));
            }
            return results;
        }
}
