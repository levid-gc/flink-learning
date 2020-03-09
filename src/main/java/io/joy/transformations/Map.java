package io.joy.transformations;

import java.util.Arrays;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/dataset_transformations.html#map">Map</a>
 * @author Owen
 */
public class Map {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    DataSet<Integer> numbers = env.fromCollection(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    DataSet<Integer> squares = numbers.map(x -> x * x);
    squares.print();
  }  
}