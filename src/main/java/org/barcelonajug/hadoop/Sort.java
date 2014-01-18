package org.barcelonajug.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.thirdparty.guava.common.base.Throwables;

import java.io.IOException;

public class Sort {

  private Job job;

  public Sort(Path inputPath, Path outputPath) {
    Configuration conf = new Configuration();

    try {
      job = new Job(conf);
      job.setJobName("Sort");
      job.setJarByClass(this.getClass());
      job.setNumReduceTasks(1);

      // Intermediate Key - Value
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(Text.class);

      // Output Key - Value
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      // Mapper & Reducer implementation
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      // job.setGroupingComparatorClass(/* Custom grouping comparator */);
      // job.setSortComparatorClass(/* Custom sort comparator */);
      // job.setPartitionerClass(/* Custom partitioner */);

      // Input
      job.setInputFormatClass(SequenceFileInputFormat.class);
      FileInputFormat.setInputPaths(job, inputPath);

      // Output
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      FileOutputFormat.setOutputPath(job, outputPath);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public void run() {
    try{
      Path outputPath = FileOutputFormat.getOutputPath(job);
      FileSystem fs = outputPath.getFileSystem(job.getConfiguration());
      fs.delete(outputPath, true);

      job.waitForCompletion(true);
      if (!job.isSuccessful()) {
        throw new RuntimeException("The job didn't finish sucessfully");
      }
    } catch(Exception e){
      throw Throwables.propagate(e);
    }
  }

  public static class Map extends Mapper<Text, IntWritable, IntWritable, Text> {

    private IntWritable outValue = new IntWritable(1);

    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
      context.write(value, key);
    }
  }

  public static class Reduce extends Reducer<IntWritable, Text, Text, IntWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for(Text value : values) {
        context.write(value, key);
      }
    }
  }

}
