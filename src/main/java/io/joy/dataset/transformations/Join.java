package io.joy.dataset.transformations;

import java.util.Arrays;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/dataset_transformations.html#join">Join</a>
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-stable/dev/java_lambdas.html#examples-and-limitations">Lambda Limitations</a>
 * @author Owen
 */
public class Join {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSet<Tuple2<Integer, String>> languages = env.fromCollection(Arrays.asList(
      new Tuple2<>(1, "C#"),
      new Tuple2<>(2, "Java"),
      new Tuple2<>(3, "Python"),
      new Tuple2<>(4, "Go")
    ));
    DataSet<Tuple2<Integer, String>> frameworks = env.fromCollection(Arrays.asList(
      new Tuple2<>(1, "ASP.NET"),
      new Tuple2<>(2, "Spring"),
      new Tuple2<>(3, "Flask"),
      new Tuple2<>(5, "Koa")
    ));

    DataSet<Tuple3<Integer, String, String>> results = languages
      .join(frameworks)
      .where(0)
      .equalTo(0)
      .with((Tuple2<Integer, String> left, Tuple2<Integer, String> right) -> 
        new Tuple3<>(left.f0, left.f1, right.f1)
      )
      .returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING));

    results.print();
  }
}