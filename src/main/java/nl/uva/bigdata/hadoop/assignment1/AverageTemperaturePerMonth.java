package nl.uva.bigdata.hadoop.assignment1;


import nl.uva.bigdata.hadoop.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class AverageTemperaturePerMonth extends HadoopJob {

  @Override
  public int run(boolean onCluster, JobConf jobConf, String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    double minimumQuality = Double.parseDouble(parsedArgs.get("--minimumQuality"));

    Job temperatures = prepareJob(onCluster, jobConf,
        inputPath, outputPath, TextInputFormat.class, MeasurementsMapper.class,
        YearMonthWritable.class, IntWritable.class, AveragingReducer.class, Text.class,
        NullWritable.class, TextOutputFormat.class);

    temperatures.getConfiguration().set("__UVA_minimumQuality", Double.toString(minimumQuality));

    temperatures.waitForCompletion(true);

    return 0;
  }

  static class YearMonthWritable implements WritableComparable {

    private int year;
    private int month;

    public YearMonthWritable() {}

    public int getYear() {
      return year;
    }

    public void setYear(int year) {
      this.year = year;
    }

    public int getMonth() {
      return month;
    }

    public void setMonth(int month) {
      this.month = month;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      // TODO Implement me
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      // TODO Implement me
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      YearMonthWritable that = (YearMonthWritable) o;
      return year == that.year && month == that.month;
    }

    @Override
    public int hashCode() {
      return Objects.hash(year, month);
    }

    @Override
    public int compareTo(Object o) {
      YearMonthWritable other = (YearMonthWritable) o;
      int byYear = Integer.compare(this.year, other.year);

      if (byYear == 0) {
        return Integer.compare(this.month, other.month);
      } else {
        return byYear;
      }
    }
  }

  public static class MeasurementsMapper extends Mapper<Object, Text, YearMonthWritable, IntWritable> {

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
      // TODO Implement me
    }
  }

  public static class AveragingReducer extends Reducer<YearMonthWritable,IntWritable,Text,NullWritable> {

    public void reduce(YearMonthWritable yearMonth, Iterable<IntWritable> temperatures, Context context)
            throws IOException, InterruptedException {
      // TODO Implement me
    }
  }
}