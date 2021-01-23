package nl.uva.bigdata.hadoop.assignment1;


import com.google.common.collect.Maps;
import nl.uva.bigdata.hadoop.HadoopClusterTestCase;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import static nl.uva.bigdata.hadoop.HadoopLocalTestCase.EPSILON;

public class AverageTemperaturePerMonthClusterTest extends HadoopClusterTestCase {

    public void test() throws Exception {

        Path inputFile = path("temperatures.tsv");
        Path outputDir = path("output");

        writeLines(inputFile, readResource("/temperatures.tsv"));

        double minimumQuality = 0.25;

        AverageTemperaturePerMonth averageTemperaturePerMonth = new AverageTemperaturePerMonth();

        averageTemperaturePerMonth.runOnCluster(createJobConf(), new String[] { "--input", inputFile.toString(),
                "--output", outputDir.toString(), "--minimumQuality", String.valueOf(minimumQuality) });

        Map<YearAndMonth, Double> results = readResults(new Path(outputDir, "part-r-00000"));

        assertEquals(results.get(new YearAndMonth(1990, 8)), 8, EPSILON);
        assertEquals(results.get(new YearAndMonth(1992, 4)), 7.888d, EPSILON);
        assertEquals(results.get(new YearAndMonth(1994, 1)), 8.24, EPSILON);
    }


    class YearAndMonth {

        private final int year;
        private final int month;

        public YearAndMonth(int year, int month) {
            this.year = year;
            this.month = month;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof YearAndMonth) {
                YearAndMonth other = (YearAndMonth) o;
                return year == other.year && month == other.month;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return 31 * year + month;
        }
    }

    private Map<YearAndMonth, Double> readResults(Path outputFile) throws IOException {
        Pattern separator = Pattern.compile("\t");
        Map<YearAndMonth,Double> averageTemperatures = Maps.newHashMap();
        for (String line : readLines(outputFile)) {
            String[] tokens = separator.split(line);
            int year = Integer.parseInt(tokens[0]);
            int month = Integer.parseInt(tokens[1]);
            double temperature = Double.parseDouble(tokens[2]);
            averageTemperatures.put(new YearAndMonth(year, month), temperature);
        }
        return averageTemperatures;
    }

}
