package com.wingify.streamer;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.wingify.streamer.streams.ClickStreamer;
import com.wingify.streamer.utils.ApplicationUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {

    public static final Properties STREAMING_PROPERTIES = new Properties();
    public static final Properties TOPIC_PROPERTIES = new Properties();

    public static void main(String[] args) throws Exception {
        loadProperties(args[0], args[1]);
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        int totalInstances = Integer.valueOf(args[2]);

        for (int i = 0; i < totalInstances; i++) {
            executorService.execute(new ClickStreamer());
        }

        startReport(ApplicationUtils.getMetricRegistry());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executorService.shutdown();
            try {
                executorService.awaitTermination(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

    public static void loadProperties(String applicationFile, String topicsFile) {
        try {
            STREAMING_PROPERTIES.load(new FileInputStream(applicationFile));
            TOPIC_PROPERTIES.load(new FileInputStream(topicsFile));

        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }

    private static void startReport(MetricRegistry metrics) {
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics).convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS).build();
        reporter.start(10, TimeUnit.SECONDS);
    }
}
