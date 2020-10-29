package assignment.client;

import assignment.statistics.RequestStatistics;
import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;
import io.swagger.client.model.SkierVertical;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@Builder
@AllArgsConstructor
public class ClientThread implements Runnable {
    private static final int MINUTES_IN_DAY = 420;
    private static final int POST_SUCCESS_CODE = 201;
    private static final int GET_SUCCESS_CODE = 200;
    private static final int GET_SUCCESS_CODE_NO_DATA = 204;

    private static final Logger logger =
            LogManager.getLogger(ClientThread.class);

    private int skierIdBegin;
    private int skierIdEnd;
    private int startTime;
    private int endTime;
    private int liftCount;
    private int day;
    private int getRequestCount;
    private int getRequestCountPhaseThree;
    private int postRequestCount;

    private AtomicInteger successCount;
    private AtomicInteger failureCount;

    private String serverAddress;
    private String resortName;

    private CountDownLatch phaseLatch;
    private CountDownLatch endLatch;

    private RequestStatistics requestStatistics;

    @Override
    public void run() {
        SkiersApi skiersApi = new SkiersApi();
        ApiClient apiClient = skiersApi.getApiClient();
        apiClient.setReadTimeout(120000);
        apiClient.setWriteTimeout(120000);
        apiClient.setConnectTimeout(120000);

        apiClient.setBasePath(serverAddress);
        LinkedList<RequestStatistics.SingleRequestStatistic> threadStats = new LinkedList<>();

        String dayString = String.valueOf(day);

        IntStream.range(0, postRequestCount)
                .forEach(val -> {
                    String randSkierId = String.valueOf(
                            ThreadLocalRandom.current().nextInt(skierIdBegin, skierIdEnd + 1));
                    String randLiftId = String.valueOf(
                            ThreadLocalRandom.current().nextInt(1, liftCount + 1));
                    String randTime = String.valueOf(
                            ThreadLocalRandom.current().nextDouble(startTime, endTime));

                    LiftRide reqBody = new LiftRide()
                            .dayID(dayString)
                            .time(randTime)
                            .skierID(randSkierId)
                            .liftID(randLiftId)
                            .resortID(resortName);

                    try {
                        long startTime = System.currentTimeMillis();

                        ApiResponse<Void> res = skiersApi.writeNewLiftRideWithHttpInfo(reqBody);

                        incrementCounts(res.getStatusCode() == POST_SUCCESS_CODE);

                        long endTime = System.currentTimeMillis();
                        RequestStatistics.SingleRequestStatistic stats =
                                RequestStatistics.SingleRequestStatistic.builder()
                                        .startTime(startTime)
                                        .endTime(endTime)
                                        .responseCode(res.getStatusCode())
                                        .requestType(RequestStatistics.POST_METHOD_NAME_ONE)
                                        .build();
                        threadStats.add(stats);
                    } catch (ApiException e) {
                        failureCount.incrementAndGet();
                        logger.log(Level.ERROR, e.getMessage());
                    }
                });

        IntStream.range(0, getRequestCount)
                .forEach(val -> {
                    String randSkierId = String.valueOf(
                            ThreadLocalRandom.current().nextInt(skierIdBegin, skierIdEnd + 1));
                    try {
                        long startTime = System.currentTimeMillis();

                        ApiResponse<SkierVertical> res =
                                skiersApi.getSkierDayVerticalWithHttpInfo(resortName, dayString, randSkierId);

                        incrementCounts(res.getStatusCode() == GET_SUCCESS_CODE
                                || res.getStatusCode() == GET_SUCCESS_CODE_NO_DATA);

                        long endTime = System.currentTimeMillis();
                        RequestStatistics.SingleRequestStatistic stats =
                                RequestStatistics.SingleRequestStatistic.builder()
                                        .startTime(startTime)
                                        .endTime(endTime)
                                        .responseCode(res.getStatusCode())
                                        .requestType(RequestStatistics.GET_METHOD_NAME_ONE)
                                        .build();
                        threadStats.add(stats);
                    } catch (ApiException e) {
                        failureCount.incrementAndGet();
                        logger.log(Level.ERROR, e.getMessage());
                    }
                });

        IntStream.range(0, getRequestCountPhaseThree)
                .forEach(val -> {
                    String randSkierId = String.valueOf(
                            ThreadLocalRandom.current().nextInt(skierIdBegin, skierIdEnd + 1));
                    try {
                        long startTime = System.currentTimeMillis();

                        ApiResponse<SkierVertical> res =
                                skiersApi.getSkierResortTotalsWithHttpInfo(
                                        randSkierId, Collections.singletonList(resortName));
                        incrementCounts(res.getStatusCode() == GET_SUCCESS_CODE
                                || res.getStatusCode() == GET_SUCCESS_CODE_NO_DATA);

                        long endTime = System.currentTimeMillis();
                        RequestStatistics.SingleRequestStatistic stats =
                                RequestStatistics.SingleRequestStatistic.builder()
                                        .startTime(startTime)
                                        .endTime(endTime)
                                        .responseCode(res.getStatusCode())
                                        .requestType(RequestStatistics.GET_METHOD_NAME_TWO)
                                        .build();
                        threadStats.add(stats);
                    } catch (ApiException e) {
                        failureCount.incrementAndGet();
                        logger.log(Level.ERROR, e.getMessage());
                    }
                });

        requestStatistics.addStatsToQueue(threadStats);
        phaseLatch.countDown();
        endLatch.countDown();
    }

    private void incrementCounts(boolean isCorrectResponse) {
        if (isCorrectResponse) {
            successCount.incrementAndGet();
        } else {
            failureCount.incrementAndGet();
        }
    }
}
