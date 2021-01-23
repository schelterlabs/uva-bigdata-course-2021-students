package nl.uva.bigdata.hadoop.assignment1;

import nl.uva.bigdata.hadoop.HadoopJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import java.util.Map;


public class BookAndAuthorReduceSideJoin extends HadoopJob {

  @Override
  public int run(boolean onCluster, JobConf jobConf, String[] args) throws Exception {

    Map<String, String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job job;

    if (onCluster) {
      job = new Job(jobConf);
    } else {
      job = new Job();
    }

    Configuration conf = job.getConfiguration();

    // TODO Implement me

    return 0;
  }

}