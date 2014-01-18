package org.barcelonajug.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.thirdparty.guava.common.base.Preconditions;

public class Example2 {

  public static void main(String... args) throws Throwable {
    Preconditions.checkState(args.length >= 2);

    Path inputPath = new Path(args[0]);
    Path wordCountOutputPath = new Path("tmp");
    Path outputPath = new Path(args[1]);
    int partitions = 1;
    if(args.length == 3){
      partitions = Integer.parseInt(args[2]);
    }


    WordCount wordCount = new WordCount(inputPath, wordCountOutputPath, partitions);
    wordCount.run();

    Sort sort = new Sort(wordCountOutputPath, outputPath);
    sort.run();
  }

}
