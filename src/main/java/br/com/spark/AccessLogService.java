package br.com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Map.Entry.comparingByKey;
import static java.util.Map.Entry.comparingByValue;

public class AccessLogService {

    private static final Pattern LOG_ACESSO_NASA = Pattern.compile("^([\\w-.]+)+" +
            "[\\s\\-\\s\\-\\s]+" +
            "[(^|\\[)]+([\\w\\d\\/\\:\\s\\-]+)+[(\\]|$)]" +
            "[\\s]+" +
            "[(^|\\\")]+(.*?)+[(\\\"|$)]+" +
            "[\\s]+" +
            "([\\d]+)+" +
            "[\\s]+" +
            "([\\d]+)?"

    );
    private static final int HOST_GROUP = 1;
    private static final int EVENT_DATE = 2;
    private static final int STATUS_CODE = 4;
    private static final int SIZE_BYTES = 5;
    private static final String STATUS_CODE_BAD_REQUEST = "404";
    private JavaSparkContext sparkContext;
    private JavaRDD<String> inputFile;

    public AccessLogService(JavaSparkContext sparkContext, String path) {
        this.sparkContext = sparkContext;
        this.inputFile = sparkContext.textFile(path).cache();
    }

    public Map<String, Integer> totalBadRequestPerDay() {
        return inputFile
                .map((Function<String, Matcher>) line -> LOG_ACESSO_NASA.matcher(line))
                .filter((Function<Matcher, Boolean>) AccessLogService::isBadRequest)
                .map((Function<Matcher, String>) matcher ->  date(matcher))
                .mapToPair(t -> new Tuple2(t, 1))
                .reduceByKey((x, y) -> (int) x + (int) y)
                .collectAsMap();
    }

    public String topFiveHostsBadRequest() {
        Map<String, Integer> map = inputFile
                .map((Function<String, Matcher>) line -> LOG_ACESSO_NASA.matcher(line))
                .filter((Function<Matcher, Boolean>) AccessLogService::isBadRequest)
                .map((Function<Matcher, String>) matcher -> matcher.group(HOST_GROUP))
                .mapToPair(t -> new Tuple2(t, 1))
                .reduceByKey((x, y) -> (int) x + (int) y)
                .collectAsMap();

        return map.entrySet().stream()
                .sorted(Collections.reverseOrder(comparingByValue()))
                .limit(5)
                .map(e -> e.getKey() + ":" + e.getValue() + " errors")
                .collect(Collectors.joining(", ", "[", "]"));
    }

    public long totalBadRequest() {
        return inputFile
                .map((Function<String, Matcher>) line -> LOG_ACESSO_NASA.matcher(line))
                .filter((Function<Matcher, Boolean>) AccessLogService::isBadRequest)
                .map((Function<Matcher, String>) matcher -> (matcher.find())
                        ? matcher.group(STATUS_CODE)
                        : "")
                .count();
    }

    public long totalBytes() {
        return inputFile
                .map((Function<String, Matcher>) line -> LOG_ACESSO_NASA.matcher(line))
                .map((Function<Matcher, Long>) matcher -> (matcher.find())
                        ? Long.parseLong(matcher.group(SIZE_BYTES) == null ? "0" : matcher.group(SIZE_BYTES))
                        : 0)
                .reduce(Long::sum);
    }

    public long uniqueHosts() {
        return inputFile
                .map((Function<String, Matcher>) line -> LOG_ACESSO_NASA.matcher(line))
                .map((Function<Matcher, String>) matcher -> (matcher.find())
                        ? matcher.group(HOST_GROUP)
                        : "")
                .mapToPair(t -> new Tuple2(t, 1))
                .reduceByKey((x, y) -> (int) x + (int) y)
                .filter((Function<Tuple2<String, Integer>, Boolean>) line -> line._2 == 1)
                .map((Function<Tuple2<String, Integer>, String>) line -> line._1)
                .count();
    }

    private static Boolean isBadRequest(Matcher matcher) {
        if (matcher.find()) {
            String statusCode = matcher.group(STATUS_CODE);
            return Objects.equals(statusCode, STATUS_CODE_BAD_REQUEST);
        }
        return false;
    }

    private static String date(Matcher matcher) {
        String eventDate = matcher.group(EVENT_DATE);
        return eventDate.substring(0, 11);
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("nasaKennedy");
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();

        String logFile = "NASA_access_log_Jul95.log";
        File file = new File(classLoader.getResource(logFile).getFile());

        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        AccessLogService accessLogService = new AccessLogService(sparkContext, file.getAbsolutePath());

        System.out.println("Número de hosts únicos: " + accessLogService.uniqueHosts());
        System.out.println("O total de erros 404: " + accessLogService.totalBadRequest());
        System.out.println("Os 5 URLs que mais causaram erro 404: " + accessLogService.topFiveHostsBadRequest());
        accessLogService.totalBadRequestPerDay().entrySet().stream()
                .sorted(comparingByKey())
                .forEach(e -> System.out.println("date: " + e.getKey()+ " total 404: " + e.getValue()));

        System.out.println("O total de bytes: " + accessLogService.totalBytes());

    }
}
