package nl.uva.bigdata.hadoop.exercise2;

import nl.uva.bigdata.hadoop.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.Map;

public class DenseMatrixVectorMultiplication extends HadoopJob {

    @Override
    public int run(boolean onCluster, JobConf jobConf, String[] args) throws Exception {

        Map<String, String> parsedArgs = parseArgs(args);

        Path matrix = new Path(parsedArgs.get("--matrix"));
        Path vector = new Path(parsedArgs.get("--vector"));
        Path outputPath = new Path(parsedArgs.get("--output"));

        Job multiplication = prepareJob(onCluster, jobConf,
                matrix, outputPath, TextInputFormat.class, RowDotProductMapper.class,
                Text.class, NullWritable.class, TextOutputFormat.class);

        // TODO Implement me

        return 0;
    }

    static class RowDotProductMapper extends Mapper<Object, Text, Text, NullWritable> {
        // TODO Implement me
    }
}