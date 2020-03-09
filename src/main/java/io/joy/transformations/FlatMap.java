package io.joy.transformations;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @see https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/dataset_transformations.html#flatmap
 * @see https://ci.apache.org/projects/flink/flink-docs-stable/dev/java_lambdas.html#examples-and-limitations
 * @see https://loremipsum.io/generator/?n=5&t=s
 * @author Owen
 */
public class FlatMap {

  public static void main(String[] args) throws Exception {
    List<String> text = new ArrayList<>();
    text.add("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod");
    text.add("tempor incididunt ut labore et dolore magna aliqua. Vestibulum morbi");
    text.add("blandit cursus risus at ultrices mi tempus. Libero justo laoreet sit amet");
    text.add("cursus sit amet dictum. Venenatis cras sed felis eget velit aliquet sagittis");
    text.add("id. Tincidunt nunc pulvinar sapien et ligula ullamcorper.");

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    DataSet<String> in = env.fromCollection(text);

    DataSet<String> out = in
      .flatMap((String line, Collector<String> collector) -> {
        for (String word : line.split("\\W")) {
          if (word.length() > 0) {
            collector.collect(word);
          }
        }
      })
      .returns(Types.STRING);

    out.print();
  }
}