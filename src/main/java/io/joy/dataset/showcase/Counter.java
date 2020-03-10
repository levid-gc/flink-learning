package io.joy.dataset.showcase;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;

/**
 * 计数器
 * 
 * @author Owen
 */
public class Counter {
  
  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    DataSet<String> languages = env.fromElements("中文", "English", "にほんご", "Français");

    DataSet<String> uLanguages = languages.map(new RichMapFunction<String, String>() {
      private static final long serialVersionUID = 1L;

      LongCounter counter = new LongCounter();

      @Override
      public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator("language-counter", counter);
      }

      @Override
      public String map(String language) throws Exception {
        counter.add(1);
        return language;
      }
    });

    String path = Counter.class.getResource("/").getPath();
    uLanguages.writeAsText(path, WriteMode.OVERWRITE).setParallelism(2);
    JobExecutionResult jobResult = env.execute("LanguageCounter");
    Long count = jobResult.getAccumulatorResult("language-counter");
    System.out.println("count = " + count);
  }
}