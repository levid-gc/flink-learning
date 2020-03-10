package io.joy.dataset.sources;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;

import io.joy.dataset.model.Employee;

/**
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/#data-sources">Data Sources</a>
 * @see <a href="https://www.microsoft.com/en-us/download/details.aspx?id=45485">Sample</a>
 * @author Owen
 */
public class CsvFile {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    String filePath = TextFile.class.getResource("/data/Import_User_Sample_en.csv").getPath();
    CsvReader reader = env
      .readCsvFile(filePath)
      .ignoreFirstLine()
      .includeFields("1000001");  // User Name + Office Number

    System.out.println("--- Tuple ---");
    reader.types(String.class, Integer.class).print();
    System.out.println("--- Employee ---");
    reader.pojoType(Employee.class, "userName", "officeNumber").print();
  }
}