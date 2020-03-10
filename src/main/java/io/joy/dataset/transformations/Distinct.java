package io.joy.dataset.transformations;

import java.util.stream.Stream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/dataset_transformations.html#distinct">Distinct</a>
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-stable/dev/java_lambdas.html#examples-and-limitations">Lambda Limitations</a>
 * @author Owen
 */
public class Distinct {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    String filePath = Distinct.class.getResource("/data/Lorem_Ipsum.txt").getPath();

    DataSet<String> lines = env.readTextFile(filePath);
    DataSet<String> words = lines
      .flatMap((String line, Collector<String> collector) -> {
        Stream.of(line.split("\\W"))
          .filter(word -> word.length() > 0)
          .forEach(word -> collector.collect(word));
      })
      .returns(Types.STRING)
      .distinct();
    
    words.print();
  }
}