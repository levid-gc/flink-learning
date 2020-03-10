package io.joy.dataset.sources;

import java.util.Arrays;

import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/#data-sources">Data Sources</a>
 * @author Owen
 */
public class Collection {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env
      .fromCollection(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
      .print();
  }
}