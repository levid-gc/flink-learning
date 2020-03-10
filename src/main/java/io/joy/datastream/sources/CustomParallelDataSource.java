package io.joy.datastream.sources;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * 自定义并行数据源
 * 
 * @author Owen
 */
public class CustomParallelDataSource implements ParallelSourceFunction<Long> {

  private static final long serialVersionUID = 1L;

  private boolean isRunning = true;
  private long count = 1;

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStreamSource<Long> source = env.addSource(new CustomParallelDataSource()).setParallelism(2);
    source.print().setParallelism(1);
    env.execute("CustomParallelDataSource");
  }

  @Override
  public void run(SourceContext<Long> ctx) throws Exception {
    while (isRunning) {
      ctx.collect(count);
      count += 1;
      Thread.sleep(1000);
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }  
}