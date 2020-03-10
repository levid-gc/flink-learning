package io.joy.dataset.sources;

import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/#data-sources">Data Sources</a>
 * @see <a href="https://loremipsum.io/generator/?n=5&t=s">Sample</a>
 * @author Owen
 */
public class TextFile {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    String filePath = TextFile.class.getResource("/data/Lorem_Ipsum.txt").getPath();
    env
      .readTextFile(filePath)
      .print();
  }
}