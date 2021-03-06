package io.joy.dataset.transformations;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/dataset_transformations.html#flatmap">FlatMap</a>
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-stable/dev/java_lambdas.html#examples-and-limitations">Lambda Limitations</a>
 * @see <a href="https://loremipsum.io/generator/?n=5&t=s">Sample</a>
 * @author Owen
 */
public class FlatMap {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    String filePath = FlatMap.class.getResource("/data/Lorem_Ipsum.txt").getPath();
    DataSet<String> lines = env.readTextFile(filePath);

    DataSet<String> words = lines
      .flatMap((String line, Collector<String> collector) -> {
        for (String word : line.split("\\W")) {
          if (word.length() > 0) {
            collector.collect(word);
          }
        }
      })
      .returns(Types.STRING);

    words.print();
  }
}