package nl.uva.bigdata.hadoop.exercise1;

import nl.uva.bigdata.hadoop.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

public class BookAndAuthorBroadcastJoin extends HadoopJob {

  @Override
  public int run(boolean onCluster, JobConf jobConf, String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job wordCount = prepareJob(onCluster, jobConf,
            books, outputPath, TextInputFormat.class, MapSideJoinMapper.class,
            Text.class, NullWritable.class, TextOutputFormat.class);

    // TODO Implement me

    return 0;
  }

  public static class MapSideJoinMapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
      // TODO Implement me
    }

  }

}