package org.gamma.config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

// ConfigSimulation.java to main sources as AutoTuner (in main) depends on it.
public class MetricSimulator { // Made public for access from AutoTuner and potentially test simulations
    private static final Logger LOGGER = Logger.getLogger(MetricSimulator.class.getName());
    private double currentCpuLoad = 30.0;
    private double currentIoWait = 5.0;
    private int currentQueueLength = 50;
    private double currentErrorRate = 1.0;

    public Map<String, Double> getMetricsForPipeline(String pipelineName) {
        currentCpuLoad += (Math.random() - 0.5) * 20;
        currentCpuLoad = Math.max(5.0, Math.min(95.0, currentCpuLoad));
        currentIoWait += (Math.random() - 0.5) * 5;
        currentIoWait = Math.max(1.0, Math.min(40.0, currentIoWait));
        if (Math.random() < 0.3) {
            currentQueueLength += (int) ((Math.random() - 0.4) * 100);
            currentQueueLength = Math.max(0, Math.min(500, currentQueueLength));
        }
        if (Math.random() < 0.1)
            currentErrorRate = Math.random() * 15;
        else if (Math.random() < 0.3)
            currentErrorRate = Math.random() * 2;

        currentErrorRate = Math.max(0.0, Math.min(20.0, currentErrorRate));

        Map<String, Double> metrics = new ConcurrentHashMap<>();
        metrics.put("systemCpuLoadPercent", currentCpuLoad);
        metrics.put("systemIoWaitPercent", currentIoWait);
        metrics.put("pipelineInputQueueLength", (double) currentQueueLength);
        metrics.put("pipelineErrorRatePercentLastHour", currentErrorRate);
        LOGGER.fine(String.format("Simulated metrics for pipeline '%s': CPU=%.1f%%, IO_Wait=%.1f%%, Queue=%d, ErrorRate=%.1f%%",
                pipelineName, currentCpuLoad, currentIoWait, currentQueueLength, currentErrorRate));
        return metrics;
    }
}
