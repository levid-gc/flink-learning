package io.joy.dataset.transformations;

import java.util.Arrays;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/dataset_transformations.html#filter">Filter</a>
 * @author Owen
 */
public class Filter {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    DataSet<Integer> numbers = env.fromCollection(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    DataSet<Integer> evenNumbers = numbers.filter(x -> x % 2 == 0);
    evenNumbers.print();
  }
}