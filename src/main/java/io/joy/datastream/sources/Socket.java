package io.joy.datastream.sources;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Socket
 * 
 * @author Owen
 */
public class Socket {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // linux: nc -lk 9999
    DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
    source.print().setParallelism(1);
    env.execute("Socket");
  }
}