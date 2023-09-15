package web.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.Arrays;

/**
 * Seems that spark needs hadoop environment for running.
 * TODO setup spark environment
 */
@Configuration
@Component
public class SparkConfig {
    @Value("${spark.app.name}")
    private String appName;

    @Value("${spark.app.master}")
    private String master;

    @Bean
    public SparkConf conf() {
        return new SparkConf().setAppName(appName).setMaster(master);
    }

    @Bean
    public JavaSparkContext context() {
        return new JavaSparkContext(conf());
    }


    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("spadas").setMaster("local"));
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3));
        long count = rdd.count();
        System.out.println(count);

    }
}

