package io.joy.transformations;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @see https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/dataset_transformations.html#first-n
 * @author Owen
 */
public class FirstN {

  public static void main(String[] args) throws Exception {
    List<Tuple2<Integer, String>> data = new ArrayList<>();
    data.add(new Tuple2<>(1, "Hadoop"));
    data.add(new Tuple2<>(1, "Spark"));
    data.add(new Tuple2<>(1, "Flink"));
    data.add(new Tuple2<>(2, "Spring"));
    data.add(new Tuple2<>(2, "ASP.NET"));
    data.add(new Tuple2<>(2, "Koa"));
    data.add(new Tuple2<>(3, "React.js"));
    data.add(new Tuple2<>(3, "Vue.js"));

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    DataSet<Tuple2<Integer, String>> in = env.fromCollection(data);
    
    // 返回数据集中的（任意）前三个元素
    DataSet<Tuple2<Integer, String>> out1 = in.first(3);
    // 返回数据集每个分组中（任意）前两个元素
    DataSet<Tuple2<Integer, String>> out2 = in.groupBy(0).first(2);
    // 返回数据集每个分组中前两个元素（元素以升序排序）
    DataSet<Tuple2<Integer, String>> out3 = in.groupBy(0).sortGroup(1, Order.ASCENDING).first(2);

    System.out.println("--- 返回数据集中的（任意）前三个元素 ---");
    out1.print();
    System.out.println("--- 返回数据集每个分组中（任意）前两个元素 ---");
    out2.print();
    System.out.println("--- 返回数据集每个分组中前两个元素（元素以升序排序） ---");
    out3.print();
  }
}