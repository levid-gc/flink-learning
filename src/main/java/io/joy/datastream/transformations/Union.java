package io.joy.datastream.transformations;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.joy.datastream.sources.CustomDataSource;

/**
 * Union
 * 
 * @author Owen
 */
public class Union {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<Long> source1 = env.addSource(new CustomDataSource());
    DataStreamSource<Long> source2 = env.addSource(new CustomDataSource());

    source1.union(source2).print().setParallelism(1);

    env.execute("Union");
  }
}