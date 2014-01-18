package org.barcelonajug.hadoop;

import org.apache.hadoop.util.ProgramDriver;

public class Launcher {

  public static void main(String... args) throws Throwable {
    ProgramDriver driver = new ProgramDriver();

    driver.addClass("example1", Example1.class, "Launches Example1");
    driver.addClass("example2", Example2.class, "Launches Example2");

    driver.driver(args);
  }

}
