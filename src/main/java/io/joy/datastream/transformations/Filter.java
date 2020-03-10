package io.joy.datastream.transformations;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.joy.datastream.sources.CustomDataSource;

/**
 * @author Owen
 */
public class Filter {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStreamSource<Long> source = env.addSource(new CustomDataSource());
    
    source
      .map(number -> {
        System.out.println("number = " + number);
        return number;
      })
      .filter(number -> number % 2 == 0)
      .print()
      .setParallelism(1);

    env.execute("Filter");
  }
}