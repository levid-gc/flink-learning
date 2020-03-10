package io.joy.dataset.transformations;

import java.util.Arrays;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/dataset_transformations.html#outerjoin">Join</a>
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-stable/dev/java_lambdas.html#examples-and-limitations">Lambda Limitations</a>
 * @author Owen
 */
public class OuterJoin {

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

    DataSet<Tuple3<Integer, String, String>> leftOuterJoin = languages
      .leftOuterJoin(frameworks)
      .where(0)
      .equalTo(0)
      .with((Tuple2<Integer, String> language, Tuple2<Integer, String> framework) -> {
        if (framework == null) {
          return new Tuple3<>(language.f0, language.f1, "-");
        } else {
          return new Tuple3<>(language.f0, language.f1, framework.f1);
        }
      })
      .returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING));

    DataSet<Tuple3<Integer, String, String>> rightOuterJoin = languages
      .rightOuterJoin(frameworks)
      .where(0)
      .equalTo(0)
      .with((Tuple2<Integer, String> language, Tuple2<Integer, String> framework) -> {
        if (language == null) {
          return new Tuple3<>(framework.f0, "-", framework.f1);
        } else {
          return new Tuple3<>(framework.f0, language.f1, framework.f1);
        }
      })
      .returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING));

    DataSet<Tuple3<Integer, String, String>> fullOuterJoin = languages
      .fullOuterJoin(frameworks)
      .where(0)
      .equalTo(0)
      .with((Tuple2<Integer, String> language, Tuple2<Integer, String> framework) -> {
        if (language == null) {
          return new Tuple3<>(framework.f0, "-", framework.f1);
        } else if (framework == null) {
          return new Tuple3<>(language.f0, language.f1, "-");
        } else {
          return new Tuple3<>(language.f0, language.f1, framework.f1);
        }
      })
      .returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING));

    System.out.println("--- left out join ---");
    leftOuterJoin.print();
    System.out.println("--- right out join ---");
    rightOuterJoin.print();
    System.out.println("--- full out join ---");
    fullOuterJoin.print();
  }
}