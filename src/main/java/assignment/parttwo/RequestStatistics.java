package assignment.parttwo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

@Getter
@NoArgsConstructor
public class RequestStatistics {
    private final String POST_METHOD_NAME = "POST";
    private final String GET_METHOD_NAME = "GET";
    private final String CSV_HEADERS =
            "RequestMethod,ResponseCode,StartTimeStamp,EndTimeStamp,Latency";

    private  static final Logger logger = LogManager.getLogger(RequestStatistics.class);

    private ConcurrentLinkedQueue<SingleRequestStatistic> postRequestStatistics = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<SingleRequestStatistic> getRequestStatistics = new ConcurrentLinkedQueue<>();

    private long meanPostLatency;
    private long meanGetLatency;
    private long medianPostLatency;
    private long medianGetLatency;
    private long p99GetResponseTime;
    private long p99PostResponseTime;
    private long maxPostResponseTime;
    private long maxGetResponseTime;

    public void generateCsvFile(String fileName) throws IOException {
        final File outputCsv = new File(fileName);

        try {
            PrintWriter printWriter = new PrintWriter(outputCsv);
            printWriter.println(CSV_HEADERS);
            postRequestStatistics
                    .forEach(stat -> printWriter.println(generateCsvLine(stat, POST_METHOD_NAME)));
            getRequestStatistics
                    .forEach(stat -> printWriter.println(generateCsvLine(stat, GET_METHOD_NAME)));
            printWriter.close();
        } catch (IOException e) {
            logger.log(Level.DEBUG, "Unable to write to csv file");
        }
    }

    private String generateCsvLine(SingleRequestStatistic stat, String methodName) {
        return methodName + "," + stat.getResponseCode() + "," + stat.getStartTime()
                + "," + stat.getEndTime() + "," + stat.getLatency();
    }

    public void calculatePerformanceMetrics() {
        ArrayList<SingleRequestStatistic> postStats = new ArrayList<>(postRequestStatistics);
        ArrayList<SingleRequestStatistic> getStats = new ArrayList<>(getRequestStatistics);

        Collections.sort(postStats);
        Collections.sort(getStats);

        maxPostResponseTime = postStats.get(postStats.size() - 1).getLatency();
        maxGetResponseTime = getStats.get(getStats.size() - 1).getLatency();

        medianPostLatency = calculateMedianLatency(postStats);
        medianGetLatency = calculateMedianLatency(getStats);

        meanGetLatency = calculateMeanLatency(getStats);
        meanPostLatency = calculateMeanLatency(postStats);

        p99GetResponseTime = calculate99Percentile(getStats);
        p99PostResponseTime = calculate99Percentile(postStats);
    }

    private long calculate99Percentile(List<SingleRequestStatistic> orderedStats) {
        int length = orderedStats.size();
        int index99 = (int) Math.ceil(0.99 * length) - 1;
        return orderedStats.get(index99).getLatency();
    }

    private long calculateMedianLatency(List<SingleRequestStatistic> stats) {
        int length = stats.size();
        if (length % 2 == 0) {
            return (stats.get(length / 2).getLatency() + stats.get(length / 2 + 1).getLatency()) / 2;
        }
        return stats.get((int)Math.floor(length / 2.0)).getLatency();
    }

    private long calculateMeanLatency(List<SingleRequestStatistic> stats) {
        return (long)stats.stream()
                .mapToDouble(SingleRequestStatistic::getLatency)
                .average()
                .orElse(Double.NaN);
    }

    @Getter
    @Builder
    @AllArgsConstructor
    public static class SingleRequestStatistic implements Comparable<SingleRequestStatistic>{
        private long startTime;
        private long endTime;
        private int responseCode;

        public long getLatency() {
            return endTime - startTime;
        }

        @Override
        public int compareTo(SingleRequestStatistic otherStat) {
            return (int)Math.ceil(this.getLatency() - otherStat.getLatency());
        }
    }
}
