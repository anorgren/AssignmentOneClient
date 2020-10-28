package assignment.statistics;

import lombok.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class StatisticsCalculator {
    private final String SEPARATOR = ",";
    private final int METHOD_TYPE_COL_INDEX = 0;
    private final int RESPONSE_CODE_COL_INDEX = 1;
    private final int START_TIME_COL_INDEX = 2;
    private final int END_TIME_COL_INDEX = 3;
    private final int LATENCY_COL_INDEX = 4;
    private final List<String> requestNames = Arrays.asList(
            RequestStatistics.GET_METHOD_NAME_ONE,
            RequestStatistics.GET_METHOD_NAME_TWO,
            RequestStatistics.POST_METHOD_NAME_ONE);

    private static final Logger logger = LogManager.getLogger(StatisticsCalculator.class);

    private String outputCsvFilePathString;
    private Path outputCsvFilePath;
    Map<String, Double> avgLatencyMap = new HashMap<>();
    Map<String, Integer> maxLatencyMap = new HashMap<>();
    Map<String, Integer> totalRequests = new HashMap<>();
    Map<String, Integer[]> countingArrayMap = new HashMap<>();
    Map<String, Integer> medianLatencyMap = new HashMap<>();
    Map<String, Integer> p99LatencyMap = new HashMap<>();

    public StatisticsCalculator(String outputCsvFilePathString) {
        this.outputCsvFilePathString = outputCsvFilePathString;
        this.outputCsvFilePath = Paths.get(outputCsvFilePathString);
    }

    private void initializeMaps() {
        requestNames.forEach(name -> {
            maxLatencyMap.put(name, -1);
            totalRequests.put(name, 0);
        });
    }

    public void calculateStats() {
        initializeMaps();
        generateMeanAndMaxResponseTimeMaps();
        generateMedianAndP99Maps();
    }

    public void generateMeanAndMaxResponseTimeMaps() {
        Map<String, Integer> totalLatency = new HashMap<>();

        requestNames.forEach(name -> {
            totalLatency.put(name, 0);
        });

        try {
            BufferedReader bufferedReader = Files.newBufferedReader(outputCsvFilePath);
            bufferedReader.readLine();
            String currentLine = bufferedReader.readLine();

            while (currentLine != null) {
                String[] csvLine = currentLine.split(SEPARATOR);
                String methodName = csvLine[METHOD_TYPE_COL_INDEX];
                Integer latency = Integer.parseInt(csvLine[LATENCY_COL_INDEX]);

                Integer currentSum = totalLatency.get(methodName);
                Integer currentCount = totalRequests.get(methodName);

                totalLatency.put(methodName, currentSum + latency);
                totalRequests.put(methodName, currentCount + 1);

                if (maxLatencyMap.get(methodName) < latency) {
                    maxLatencyMap.put(methodName, latency);
                }

                currentLine = bufferedReader.readLine();
            }

            totalRequests.forEach((key, value) -> {
                Integer sum = totalLatency.get(key);
                Double avg = (double) (sum / value);
                avgLatencyMap.put(key, avg);
            });
        } catch (IOException e) {
            logger.log(Level.FATAL, e.getMessage());
        }
    }

    private void createCountingArraysForKthValue() {
        if (this.maxLatencyMap.isEmpty()) generateMeanAndMaxResponseTimeMaps();

        this.maxLatencyMap.forEach((key, value) -> {
            int maxSize = value + 1;
            countingArrayMap.put(key, new Integer[maxSize]);
        });

        try {
            BufferedReader bufferedReader = Files.newBufferedReader(outputCsvFilePath);
            bufferedReader.readLine();
            String csvLine = bufferedReader.readLine();

            while (csvLine != null) {
                String[] data = csvLine.split(SEPARATOR);
                String requestType = data[METHOD_TYPE_COL_INDEX];
                int latency = Integer.parseInt(data[LATENCY_COL_INDEX]);

                Integer[] methodCountingArray = countingArrayMap.get(requestType);

                if (methodCountingArray[latency] == null) methodCountingArray[latency] = 0;
                methodCountingArray[latency]++;

                csvLine = bufferedReader.readLine();
            }
        } catch (IOException e) {
            logger.log(Level.FATAL, e.getMessage());
        }
    }

    private void generateMedianAndP99Maps() {
        if (countingArrayMap.isEmpty()) createCountingArraysForKthValue();

        countingArrayMap.forEach((key, countingArray) -> {
            int totalReqCount = totalRequests.get(key);

            Integer medianVal =
                    calculatePercentileValue(countingArray, totalReqCount, 0.5);
            Integer p99Val =
                    calculatePercentileValue(countingArray, totalReqCount, 0.99);

            medianLatencyMap.put(key, medianVal);
            p99LatencyMap.put(key, p99Val);
        });
    }

    private Integer calculatePercentileValue(
            Integer[] countingArray, int totalReqCount, double percentile) {
        int kthIndex = (int)Math.round(totalReqCount * percentile);
        int totalCount = totalReqCount;

        for (int i = countingArray.length - 1; i >= 0; i--) {
            if (countingArray[i] != null) {
                totalCount -= countingArray[i];
            }
            if (totalCount <= kthIndex) {
                return i;
            }
        }
        return -1;
    }
}
