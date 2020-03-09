package io.joy.transformations;

import java.util.stream.Stream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/dataset_transformations.html#mappartition">MapPartition</a>
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-stable/dev/java_lambdas.html#examples-and-limitations">Lambda Limitations</a>
 * @see <a href="https://loremipsum.io/generator/?n=5&t=s">Sample</a>
 * @author Owen
 */
public class MapPartition {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    String filePath = FlatMap.class.getResource("/data/Lorem_Ipsum.txt").getPath();
    DataSet<String> lines = env.readTextFile(filePath);

    DataSet<Long> wordsPerLine = lines
      .mapPartition((Iterable<String> iLines, Collector<Long> collector) -> {
        for (String line : iLines) {
          String[] words = line.split("\\W");
          Long count = Stream.of(words).filter(word -> word.length() > 0).count();
          collector.collect(count);
        }
      })
      .returns(Types.LONG);

    wordsPerLine.print();
  }
}