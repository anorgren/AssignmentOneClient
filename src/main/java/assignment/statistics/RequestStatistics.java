package assignment.statistics;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
@NoArgsConstructor
public class RequestStatistics {
    public static final String GET_METHOD_NAME_ONE = "GET1";
    public static final String GET_METHOD_NAME_TWO = "GET2";
    public static final String POST_METHOD_NAME_ONE = "POST";

    private  static final Logger logger = LogManager.getLogger(RequestStatistics.class);

    private String fileName;
    private BlockingQueue<Collection<SingleRequestStatistic>> requestWriteQueue = new LinkedBlockingQueue<>();
    private CsvWriter csvWriter;
    private StatisticsCalculator statisticsCalculator;

    private Double meanPostLatency;
    private Double meanGet1Latency;
    private Double meanGet2Latency;
    private long medianPostLatency;
    private long medianGet1Latency;
    private long medianGet2Latency;
    private long p99Get1ResponseTime;
    private long p99Get2ResponseTime;
    private long p99PostResponseTime;
    private long maxPostResponseTime;
    private long maxGet1ResponseTime;
    private long maxGet2ResponseTime;

    public RequestStatistics(String outputFileName) {
        this.fileName = outputFileName;
        this.csvWriter = new CsvWriter(fileName, requestWriteQueue);
    }

    public Thread startWritingToCsv() {
        return csvWriter.startWriter();
    }

    public void startCalculation() {
        statisticsCalculator = new StatisticsCalculator(fileName);
        statisticsCalculator.calculateStats();
    }

    public void setVals() {
        Map<String, Double> avgLatencyMap = statisticsCalculator.getAvgLatencyMap();
        this.meanGet1Latency = avgLatencyMap.get(GET_METHOD_NAME_ONE);
        this.meanGet2Latency = avgLatencyMap.get(GET_METHOD_NAME_TWO);
        this.meanPostLatency = avgLatencyMap.get(POST_METHOD_NAME_ONE);

        Map<String, Integer> maxLatencyMap = statisticsCalculator.getMaxLatencyMap();
        this.maxPostResponseTime = maxLatencyMap.get(POST_METHOD_NAME_ONE);
        this.maxGet1ResponseTime = maxLatencyMap.get(GET_METHOD_NAME_ONE);
        this.maxGet2ResponseTime = maxLatencyMap.get(GET_METHOD_NAME_TWO);

        Map<String, Integer> medianLatencyMap = statisticsCalculator.getMedianLatencyMap();
        this.medianGet1Latency = medianLatencyMap.get(GET_METHOD_NAME_ONE);
        this.medianGet2Latency = medianLatencyMap.get(GET_METHOD_NAME_TWO);
        this.medianPostLatency = medianLatencyMap.get(POST_METHOD_NAME_ONE);

        Map<String, Integer> p99Map = statisticsCalculator.getP99LatencyMap();
        this.p99Get1ResponseTime = p99Map.get(GET_METHOD_NAME_ONE);
        this.p99Get2ResponseTime = p99Map.get(GET_METHOD_NAME_TWO);
        this.p99PostResponseTime = p99Map.get(POST_METHOD_NAME_ONE);
    }

    public void addStatsToQueue(Collection<SingleRequestStatistic> statsToAdd) {
        try {
            requestWriteQueue.put(statsToAdd);
        } catch (InterruptedException e) {
            logger.log(Level.FATAL, e.getMessage());
        }
    }

    @Getter
    @Builder
    @AllArgsConstructor
    public static class SingleRequestStatistic implements Comparable<SingleRequestStatistic>{
        private long startTime;
        private long endTime;
        private int responseCode;
        private String requestType;

        public long getLatency() {
            return endTime - startTime;
        }

        @Override
        public int compareTo(SingleRequestStatistic otherStat) {
            return (int)Math.ceil(this.getLatency() - otherStat.getLatency());
        }
    }
}
