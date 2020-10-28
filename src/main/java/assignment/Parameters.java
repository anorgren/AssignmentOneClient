package assignment;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

@Getter
@Builder
@ToString
@AllArgsConstructor
public class Parameters {
    private static final String MAX_THREADS_PROP_NAME = "maxThreads";
    private static final String NUM_SKIERS_PROP_NAME = "numSkiers";
    private static final String NUM_SKI_LIFTS_PROP_NAME = "numLifts";
    private static final String SKI_DAY_PROP_NAME = "skiDay";
    private static final String RESORT_NAME_PROP_NAME = "resortName";
    private static final String SERVER_ADDRESS_PROP_NAME = "hostServerAddress";
    private static final List<String> REQUIRED_PROPERTIES = Arrays.asList(
            NUM_SKI_LIFTS_PROP_NAME,
            NUM_SKIERS_PROP_NAME,
            RESORT_NAME_PROP_NAME,
            SERVER_ADDRESS_PROP_NAME,
            MAX_THREADS_PROP_NAME);
    private static final List<String> REQUIRED_PROPERTIES_WITH_INT_VALUES =
            Arrays.asList(NUM_SKIERS_PROP_NAME, MAX_THREADS_PROP_NAME, NUM_SKI_LIFTS_PROP_NAME, SKI_DAY_PROP_NAME);

    private static final int MAX_SKI_DAY = 366;
    private static final int MIN_NUM_LIFTS = 5;
    private static final int MAX_NUM_LIFTS = 60;
    private static final String DEFAULT_RESORT_ID = "SilverMt";
    private static final String DEFAULT_LIFT_COUNT = "40";
    private static final String DEFAULT_SKIER_COUNT = "50000";
    private static final String DEFAULT_DAY = "1";
    private static final int MINIMUM_THREAD_COUNT_LIMIT = 4;
    private static final int MAXIMUM_THREAD_COUNT_LIMIT = 256;
    private static final String DEFAULT_FILE_NAME = "output_file";
    private static final String DEFAULT_SERVER_ADDRESS =
            "http://ec2-18-236-236-150.us-west-2.compute.amazonaws.com:8080/SkiServerWar";

    private int maxThreadCount;
    private int skierCount;
    private int liftCount;
    private int skiDayNumber;
    private String resortId;
    private String hostServerAddress;

    public static Optional<Parameters> parsePropertiesFile(String filePath) {
        Properties properties = new Properties();

        try {
            InputStream is = new FileInputStream("client_config.properties");
            properties.load(is);
            return Optional.of(propertiesToParameters(properties));
        } catch (FileNotFoundException fnfe) {
            System.out.println(fnfe.getMessage());
        } catch (IOException ioe) {
            System.out.println("Could not read properties file");
        }
        return Optional.of(createDefaultParameters());
    }

    private static Parameters propertiesToParameters(Properties properties) {
        if (requiredPropertiesPresent(properties)) {
            int skierCount = Integer.parseInt(properties.getProperty(NUM_SKIERS_PROP_NAME, DEFAULT_SKIER_COUNT));
            int liftCount = Integer.parseInt(properties.getProperty(NUM_SKI_LIFTS_PROP_NAME, DEFAULT_LIFT_COUNT));
            int skiDayNumber = Integer.parseInt(properties.getProperty(SKI_DAY_PROP_NAME, DEFAULT_DAY));
            int maxThreadCount = Integer.parseInt(properties.getProperty(MAX_THREADS_PROP_NAME));

            String resort = properties.getProperty(RESORT_NAME_PROP_NAME, DEFAULT_RESORT_ID);
            String serverAddress = properties.getProperty(SERVER_ADDRESS_PROP_NAME, DEFAULT_SERVER_ADDRESS);

            return Parameters.builder()
                    .hostServerAddress(serverAddress)
                    .liftCount(liftCount)
                    .skierCount(skierCount)
                    .skiDayNumber(skiDayNumber)
                    .maxThreadCount(maxThreadCount)
                    .resortId(resort)
                    .build();
        }
        throw new IllegalArgumentException("Invalid Properties Provided.");
    }

    private static boolean requiredPropertiesPresent(Properties properties) {
        boolean allPropsPresent = properties.stringPropertyNames().containsAll(REQUIRED_PROPERTIES);
        boolean allIntPropsPresent = REQUIRED_PROPERTIES_WITH_INT_VALUES.stream()
                .allMatch(intProp -> validateIntegerProperty(intProp, properties.getProperty(intProp)));

        return allPropsPresent && allIntPropsPresent;
    }

    private static boolean validateIntegerProperty(String propName, String rawPropertyValue) {
        if (rawPropertyValue != null) {
            try {
                int value = Integer.parseInt(rawPropertyValue);
                switch(propName) {
                    case MAX_THREADS_PROP_NAME:
                        return value >= MINIMUM_THREAD_COUNT_LIMIT && value <= MAXIMUM_THREAD_COUNT_LIMIT;
                    case SKI_DAY_PROP_NAME:
                        return value >= 1 && value <= MAX_SKI_DAY;
                    case NUM_SKIERS_PROP_NAME:
                        return value >= 0;
                    case NUM_SKI_LIFTS_PROP_NAME:
                        return value >= MIN_NUM_LIFTS && value <= MAX_NUM_LIFTS;
                    default:
                        throw new IllegalArgumentException("Unknown property present.");
                }
            } catch (NumberFormatException nfe) {
                System.out.println(propName + " should be an integer. Unable to parse properties file.");
                return false;
            }
        }
        return true;
    }

    private static Parameters createDefaultParameters() {
        return Parameters.builder()
                .resortId(DEFAULT_RESORT_ID)
                .maxThreadCount(256)
                .skiDayNumber(Integer.parseInt(DEFAULT_DAY))
                .liftCount(Integer.parseInt(DEFAULT_LIFT_COUNT))
                .hostServerAddress(DEFAULT_SERVER_ADDRESS)
                .skierCount(Integer.parseInt(DEFAULT_SKIER_COUNT))
                .build();
    }
}
