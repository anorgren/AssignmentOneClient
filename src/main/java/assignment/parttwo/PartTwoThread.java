package assignment.parttwo;


import assignment.partone.PartOneClient;
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@Builder
@AllArgsConstructor
public class PartTwoThread implements Runnable {
    private static final int MINUTES_IN_DAY = 420;
    private static final int POST_SUCCESS_CODE = 201;
    private static final int GET_SUCCESS_CODE = 200;
    private static final String DEFAULT_DAY = "42";

    private static final Logger logger =
            LogManager.getLogger(PartTwoThread.class);

    private int skierIdBegin;
    private int skierIdEnd;
    private int startTime;
    private int endTime;
    private int liftCount;
    private int day;
    private int getRequestCount;
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
        apiClient.setBasePath(serverAddress);

        String dayString = String.valueOf(day);

        IntStream.range(0, postRequestCount)
                .forEach(val -> {
                    String randSkierId = String.valueOf(
                            ThreadLocalRandom.current().nextInt(skierIdBegin, skierIdEnd + 1));
                    String randLiftId = String.valueOf(
                            ThreadLocalRandom.current().nextInt(1, liftCount + 1));
                    String randTime = String.valueOf(
                            ThreadLocalRandom.current().nextInt(startTime, endTime + 1));

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
                                        .build();
                        requestStatistics.getPostRequestStatistics().add(stats);
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
                        incrementCounts(res.getStatusCode() == GET_SUCCESS_CODE);

                        long endTime = System.currentTimeMillis();
                        RequestStatistics.SingleRequestStatistic stats =
                                RequestStatistics.SingleRequestStatistic.builder()
                                        .startTime(startTime)
                                        .endTime(endTime)
                                        .responseCode(res.getStatusCode())
                                        .build();
                        requestStatistics.getGetRequestStatistics().add(stats);
                    } catch (ApiException e) {
                        logger.log(Level.ERROR, e.getMessage());
                    }
                });

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
