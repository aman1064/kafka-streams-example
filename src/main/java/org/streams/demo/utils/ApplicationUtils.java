package org.streams.demo.utils;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;


/*
This is a utility classs that defines dropwizard metric registry and is used for metric collection.
Meters are used to measure speed at which data is getting processed.
 */
public class ApplicationUtils {

    private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
    private static final Meter CLICK_METER = METRIC_REGISTRY.meter("raw-clicks-incoming-rate");

    public static MetricRegistry getMetricRegistry() {
        return METRIC_REGISTRY;
    }

    public static Meter getClickMeter() {
        return CLICK_METER;
    }
}
