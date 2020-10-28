package assignment.statistics;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;

public class CsvWriter {
    private final String CSV_HEADERS =
            "RequestMethod,ResponseCode,StartTimeStamp,EndTimeStamp,Latency";

    private String fileName;
    private PrintWriter printWriter;
    private BlockingQueue<Collection<RequestStatistics.SingleRequestStatistic>> statsQueue;

    private static final Logger logger = LogManager.getLogger(CsvWriter.class);

    CsvWriter(String fileName, BlockingQueue<Collection<RequestStatistics.SingleRequestStatistic>> statsQueue) {
        this.fileName = fileName;
        this.statsQueue = statsQueue;
    }

    public Thread startWriter() {
        createFile();
        return createWriteThread();
    }

    public void createFile() {
        File outputFile = new File(fileName);
        try {
            printWriter = new PrintWriter(outputFile);
            printWriter.println(CSV_HEADERS);
        } catch (FileNotFoundException e) {
            logger.log(Level.FATAL, e.getMessage());
        }
    }

    private String generateCsvLine(RequestStatistics.SingleRequestStatistic stat) {
        return stat.getRequestType() + "," + stat.getResponseCode() + "," + stat.getStartTime()
                + "," + stat.getEndTime() + "," + stat.getLatency();
    }

    private void writeThreadData() {
        try {
            Collection<RequestStatistics.SingleRequestStatistic> reqStats = statsQueue.take();
            while (reqStats.size() > 0) {
                reqStats.forEach(stats -> {
                    String csvLine = generateCsvLine(stats);
                    printWriter.println(csvLine);
                });
                reqStats = statsQueue.take();
            }
            printWriter.close();
        } catch (InterruptedException e) {
            logger.log(Level.FATAL, e.getMessage());
        }
    }

    public Thread createWriteThread() {
        Runnable thread = this::writeThreadData;

        Thread writeThread = new Thread(thread);
        writeThread.start();
        return writeThread;
    }

}
