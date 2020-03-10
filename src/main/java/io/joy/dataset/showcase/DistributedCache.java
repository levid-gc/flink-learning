package io.joy.dataset.showcase;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

/**
 * 读取分布式缓存
 * 
 * @author Owen
 */
public class DistributedCache {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    String filePath = DistributedCache.class.getResource("/data/Lorem_Ipsum.txt").getPath();
    
    // 注册本地缓存
    env.registerCachedFile(filePath, "dc-file");

    DataSet<String> languages = env.fromElements("中文", "English", "にほんご", "Français");

    languages
      .map(new RichMapFunction<String, String>() {
        private static final long serialVersionUID = 1L;

        @Override
        public void open(Configuration parameters) throws Exception {
          File file = getRuntimeContext().getDistributedCache().getFile("dc-file");
          List<String> lines = FileUtils.readLines(file);
          for (String line : lines) {
            System.out.println("line: [" + line + "]");
          }
        }

        @Override
        public String map(String language) throws Exception {
          return language;
        }
      })
      .print();
  }
}