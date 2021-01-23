package nl.uva.bigdata.hadoop.exercise1;

import nl.uva.bigdata.hadoop.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

public class InvertedIndex extends HadoopJob {

    @Override
    public int run(boolean onCluster, JobConf jobConf, String[] args) throws Exception {

        Map<String,String> parsedArgs = parseArgs(args);

        Path inputPath = new Path(parsedArgs.get("--input"));
        Path outputPath = new Path(parsedArgs.get("--output"));

        Job invertedIndex = prepareJob(onCluster, jobConf,
                inputPath, outputPath, TextInputFormat.class, PageParser.class,
                Text.class, IntWritable.class, IndexEntryBuilder.class, Text.class, NullWritable.class,
                TextOutputFormat.class);
        invertedIndex.waitForCompletion(true);


        return 0;
    }

    public static class PageParser extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            // TODO Implement me
        }
    }

    public static class IndexEntryBuilder extends Reducer<Text,IntWritable,Text,NullWritable> {

        public void reduce(Text word, Iterable<IntWritable> pageIds, Context context)
                throws IOException, InterruptedException {
            // TODO Implement me
        }
    }

}
