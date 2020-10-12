package assignment.parttwo;

import assignment.Parameters;
import assignment.partone.PartOneClient;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;


public class PartTwoClient {
    private static final String CONFIG_FILE_PATH = "client_config.properties";
    private static final double MILLISECONDS_IN_SECOND = 1000.0;

    private static final int MAX_THREAD_DENOMINATOR = 4;

    private static final int PHASE_ONE_TIME_START = 1;
    private static final int PHASE_ONE_TIME_END = 90;
    private static final int PHASE_TWO_TIME_START = 91;
    private static final int PHASE_TWO_TIME_END = 360;
    private static final int PHASE_THREE_TIME_START = 361;
    private static final int PHASE_THREE_TIME_END = 420;

    private static final int PHASE_ONE_GET_REQ_COUNT = 5;
    private static final int PHASE_ONE_POST_REQ_COUNT = 100;
    private static final int PHASE_TWO_GET_REQ_COUNT = 5;
    private static final int PHASE_TWO_POST_REQ_COUNT = 100;
    private static final int PHASE_THREE_GET_REQ_COUNT = 10;
    private static final int PHASE_THREE_POST_REQ_COUNT = 100;


    private static final Logger logger =
            LogManager.getLogger(PartOneClient.class);

    public static void main(String[] args) throws InterruptedException, IOException{
        logger.log(Level.INFO, "Client Part Two Starting...........");
        Optional<Parameters> clientParams = Parameters.parsePropertiesFile(CONFIG_FILE_PATH);

        if(clientParams.isPresent()) {
            RequestStatistics requestStatistics = new RequestStatistics();
            final Parameters parameters = clientParams.get();

            int maxThreads = parameters.getMaxThreadCount();
            int totalThreads = maxThreads + maxThreads/2;
            int phaseOneThreads = maxThreads / MAX_THREAD_DENOMINATOR;
            int phaseThreeThreads = maxThreads / MAX_THREAD_DENOMINATOR;

            int phaseOneCountDownEnd = ((int) Math.ceil(phaseOneThreads / 10.0));
            int phaseTwoCountDownEnd = ((int) Math.ceil(maxThreads / 10.0));

            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failureCount = new AtomicInteger(0);

            CountDownLatch totalCountDownLatch = new CountDownLatch(totalThreads);
            CountDownLatch phaseOneLatch = new CountDownLatch(phaseOneCountDownEnd);
            CountDownLatch phaseTwoLatch = new CountDownLatch(phaseTwoCountDownEnd);
            CountDownLatch phaseThreeLatch = new CountDownLatch(0);

            long programStartTime = System.currentTimeMillis();

            logger.log(Level.INFO, "Phase One Beginning");
            createThreads(
                    parameters,
                    phaseOneLatch,
                    totalCountDownLatch,
                    phaseOneThreads,
                    PHASE_ONE_TIME_START,
                    PHASE_ONE_TIME_END,
                    PHASE_ONE_GET_REQ_COUNT,
                    PHASE_ONE_POST_REQ_COUNT,
                    successCount,
                    failureCount,
                    requestStatistics);

            phaseOneLatch.await();

            logger.log(Level.INFO, "Phase Two Beginning");
            createThreads(
                    parameters,
                    phaseTwoLatch,
                    totalCountDownLatch,
                    maxThreads,
                    PHASE_TWO_TIME_START,
                    PHASE_TWO_TIME_END,
                    PHASE_TWO_GET_REQ_COUNT,
                    PHASE_TWO_POST_REQ_COUNT,
                    successCount,
                    failureCount,
                    requestStatistics);

            phaseTwoLatch.await();

            logger.log(Level.INFO, "Phase Three Beginning");
            createThreads(
                    parameters,
                    phaseThreeLatch,
                    totalCountDownLatch,
                    phaseThreeThreads,
                    PHASE_THREE_TIME_START,
                    PHASE_THREE_TIME_END,
                    PHASE_THREE_GET_REQ_COUNT,
                    PHASE_THREE_POST_REQ_COUNT,
                    successCount,
                    failureCount,
                    requestStatistics);

            totalCountDownLatch.await();

            logger.log(Level.INFO, "Client processed all requests");
            long endTime = System.currentTimeMillis();

            requestStatistics.calculatePerformanceMetrics();
            requestStatistics.generateCsvFile("phaseTwo4096Threads.csv");

            printResults(requestStatistics, parameters, programStartTime, endTime, successCount, failureCount);
        } else {
            logger.log(Level.DEBUG, "Unable to load parameters config file");
        }
        logger.log(Level.INFO, "Client shutting down..........");
    }

    private static void createThreads(
            Parameters parameters,
            CountDownLatch phaseLatch,
            CountDownLatch endLatch,
            int numberThreads,
            int startTime,
            int endTime,
            int getRequestCount,
            int postRequestCount,
            AtomicInteger successCount,
            AtomicInteger failureCount,
            RequestStatistics requestStatistics) {

        int numberSkiers = parameters.getSkierCount();
        int maxThreads = parameters.getMaxThreadCount();

        IntStream.range(0, numberThreads)
                .forEach( i -> {
                    int numberSkiersPerThread = numberSkiers / maxThreads / MAX_THREAD_DENOMINATOR;
                    int skierIdStart = i * numberSkiersPerThread + 1;
                    int skierIdStop = (i + 1) * numberSkiersPerThread;

                    PartTwoThread clientThread =
                            PartTwoThread.builder()
                                    .serverAddress(parameters.getHostServerAddress())
                                    .day(parameters.getSkiDayNumber())
                                    .phaseLatch(phaseLatch)
                                    .endLatch(endLatch)
                                    .getRequestCount(getRequestCount)
                                    .postRequestCount(postRequestCount)
                                    .liftCount(parameters.getLiftCount())
                                    .startTime(startTime)
                                    .endTime(endTime)
                                    .skierIdBegin(skierIdStart)
                                    .skierIdEnd(skierIdStop)
                                    .resortName(parameters.getResortId())
                                    .successCount(successCount)
                                    .failureCount(failureCount)
                                    .requestStatistics(requestStatistics)
                                    .build();

                    (new Thread(clientThread)).start();
                });
    }

    private static void printResults(
            RequestStatistics requestStatistics,
            Parameters parameters,
            long startTime,
            long endTime,
            AtomicInteger successCount,
            AtomicInteger failureCount) {

        double wallTime = (endTime - startTime)/MILLISECONDS_IN_SECOND;
        double throughput = (successCount.get() + failureCount.get()) / wallTime;

        System.out.println("Max Threads: " + parameters.getMaxThreadCount());
        System.out.println("Number of Successful Requests Sent: " + successCount);
        System.out.println("Number of Unsuccessful Requests: " + failureCount);
        System.out.println("Total Wall Time(s): " + wallTime);
        System.out.println("Throughput (req/s): " + throughput);
        System.out.println("Mean POST response time(ms): " + requestStatistics.getMeanPostLatency());
        System.out.println("Mean GET response time(ms): " + requestStatistics.getMeanGetLatency());
        System.out.println("Median POST response time(ms): " + requestStatistics.getMedianPostLatency());
        System.out.println("Median GET response time(ms): " + requestStatistics.getMedianGetLatency());
        System.out.println("Max POST response time(ms): " + requestStatistics.getMaxPostResponseTime());
        System.out.println("Max GET response time(ms): " + requestStatistics.getMaxGetResponseTime());
        System.out.println("99th Percentile POST response time(ms): " + requestStatistics.getP99PostResponseTime());
        System.out.println("99th Percentile GET response time(ms): " + requestStatistics.getP99GetResponseTime());
    }
}
