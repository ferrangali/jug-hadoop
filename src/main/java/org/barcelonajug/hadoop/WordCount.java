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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.thirdparty.guava.common.base.Throwables;

import java.io.IOException;

public class WordCount {

  private Job job;

  public WordCount(Path inputPath, Path outputPath, int partitions) {
    Configuration conf = new Configuration();

    try {
      job = new Job(conf);
      job.setJobName("WordCount");
      job.setJarByClass(this.getClass());
      job.setNumReduceTasks(partitions);

      // Intermediate Key - Value
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);

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
      job.setInputFormatClass(TextInputFormat.class);
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

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outKey = new Text();
    private IntWritable outValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      context.getCounter("CUSTOM_COUNTERS", "LINES").increment(1L);

      String[] words = line.split(" ");

      for(String word : words){
        outKey.set(word);
        context.write(outKey, outValue);

        context.getCounter("CUSTOM_COUNTERS", "WORDS").increment(1L);
      }
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{

    private IntWritable outValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      context.getCounter("CUSTOM_COUNTERS", "GROUPS").increment(1L);

      int count = 0;
      for(IntWritable value : values){
        count += value.get();
      }
      outValue.set(count);
      context.write(key, outValue);
    }
  }

}
