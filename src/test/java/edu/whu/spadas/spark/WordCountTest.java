package edu.whu.spadas.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.ResourceUtils;
import web.SpadasWebApplication;
import web.config.SparkConfig;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RunWith(SpringRunner.class)
@Slf4j
@Import(SparkConfig.class)
@SpringBootTest(classes = SparkConfig.class)
public class WordCountTest {
    @Autowired
    private JavaSparkContext sc;

    private final String textFile = "article.txt";

    @Test
    public void wordCountTest() throws IOException {
        // read the content
        File file = ResourceUtils.getFile("classpath:" + textFile);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String allContent = reader.lines().reduce((a, b) -> a + " " + b).orElse("a");
        // split words
        String reg = "(\\w+)";
        Pattern pattern = Pattern.compile(reg);
        Matcher matcher = pattern.matcher(allContent);
        List<String> words = new ArrayList<>();
        while (matcher.find()) {
            words.add(matcher.group());
        }
        // word count with spark
        JavaRDD<String> rdd = sc.parallelize(words);
        Map<String, Long> result = rdd.countByValue();
        // print
        for (Map.Entry<String, Long> entry : result.entrySet()) {
            log.info("{} -> {}", entry.getKey(), entry.getValue());
        }
    }
}
