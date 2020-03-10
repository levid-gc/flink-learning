package io.joy.dataset.transformations;

import java.util.Arrays;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/dataset_transformations.html#cross">Cross</a>
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-stable/dev/java_lambdas.html#examples-and-limitations">Lambda Limitations</a>
 * @author Owen
 */
public class Cross {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSet<String> students = env.fromCollection(Arrays.asList("Joy", "Owen"));
    DataSet<String> courses = env.fromCollection(Arrays.asList("Flink", "Spring", "React"));

    DataSet<Tuple2<String, String>> result = students
      .cross(courses)
      .with((String student, String course) -> new Tuple2<>(student, course))
      .returns(Types.TUPLE(Types.STRING, Types.STRING));

    result.print();
  }
}