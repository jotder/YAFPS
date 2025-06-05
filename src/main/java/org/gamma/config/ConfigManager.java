package org.gamma.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

record DatabaseConnectionItem(String connectionName, String type, String dbUrl, String dbUser, String dbPass,
                              Integer maxPool, Long idleTimeout, Long connTimeout, Long maxLifetime) {
}

record PipelineDetailsConfig(String pipelineName, List<SourceDetailsItem> sourcesDetails, List<OutputItem> outputs) {
}

record SourceDetailsItem(String sourceId, String sourceMetadata, String sourceFields,
                         ValidationRulesItem validationRules) {
}

record FieldValidationConfigItem(String fieldName, List<ValidationExpressionItem> validations) {
}

record ValidationExpressionItem(String expression, String errorMessage, String actionOnFailure) {
}

record TransformationItem(String transformationName, String type, String expression) {
}

record AggregationDetailItem(@JsonProperty("input_field") String inputField, String function) {
}

record AdjustableParametersConfig(AdjustableParameterDetailConfig numThreads,
                                  AdjustableParameterDetailConfig batchSize) {
}

record AdjustableParameterDetailConfig(Integer min, Integer max, Integer incrementStep, Integer decrementStep,
                                       Double incrementFactor, Double decrementFactor) {
}

record TuningRuleConfig(String ruleName, String metric, String condition, Double threshold, String conditionLogic,
                        List<SingleConditionConfig> conditions, Integer consecutiveBreachesToTrigger,
                        RuleActionConfig action) {
}

record SingleConditionConfig(String metric, String condition, Double threshold) {
}

record RuleActionConfig(String type, Integer durationMinutes, RuleActionParamsConfig params,
                        RuleActionConfig interimAction, String reason) {
}

record RuleActionParamsConfig(Integer numThreadsIncrement, Integer numThreadsDecrement, String targetNumThreads,
                              Double batchSizeIncrementFactor, Double batchSizeDecrementFactor,
                              Double targetBatchSizeFactor) {
}

record SourceRuntimeState(String sourceId, AtomicInteger currentNumThreads, AtomicInteger currentBatchSize,
                          AtomicBoolean currentActive) {
    public SourceRuntimeState(SourceItem sourceConfig) { // Takes simplified SourceItem
        this(sourceConfig.sourceId(), new AtomicInteger(sourceConfig.numThreads() != null ? sourceConfig.numThreads() : 1),
                new AtomicInteger(sourceConfig.batchSize() != null ? sourceConfig.batchSize() : 10),
                new AtomicBoolean(sourceConfig.active()));
    }
}

record PipelineRuntimeState(String pipelineName, AtomicBoolean currentActive,
                            Map<String, SourceRuntimeState> sourceStates, AutoTuningConfig tuningConfig,
                            PipelineDetailsConfig detailsConfig,
                            Map<String, AtomicInteger> ruleConsecutiveBreaches, long pipelineCooldownUntilTimestamp) {
    public PipelineRuntimeState(EtlPipelineItem pipelineConfig, PipelineDetailsConfig detailsConfig) {
        this(pipelineConfig.pipelineName(), new AtomicBoolean(pipelineConfig.active()),
                new ConcurrentHashMap<>(), pipelineConfig.autoTuning(), detailsConfig, // Store the loaded details
                new ConcurrentHashMap<>(), 0L);
        if (pipelineConfig.sources() != null) {
            for (SourceItem sourceCoreConfig : pipelineConfig.sources())
                this.sourceStates.put(sourceCoreConfig.sourceId(), new SourceRuntimeState(sourceCoreConfig));
        }
        if (this.tuningConfig != null && this.tuningConfig.tuningRules() != null) {
            for (TuningRuleConfig rule : this.tuningConfig.tuningRules())
                this.ruleConsecutiveBreaches.put(rule.ruleName(), new AtomicInteger(0));
        }
    }
}

class MetricSimulator {
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


public class ConfigManager {
    private static final Logger APP_LOGGER = Logger.getLogger(ConfigManager.class.getName());

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT] [%4$-7s] %3$s - %5$s %6$s%n");
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter());
        handler.setLevel(Level.ALL);
        Logger rootLogger = Logger.getLogger("");
        rootLogger.addHandler(handler);
        rootLogger.setLevel(Level.INFO);
        Logger.getLogger(RuntimeConfigManager.class.getName()).setLevel(Level.INFO);
        Logger.getLogger(AutoTuner.class.getName()).setLevel(Level.INFO);
        Logger.getLogger(MetricSimulator.class.getName()).setLevel(Level.FINE);
    }

    static AppConfig appConfig;

    public static void main(String[] args) {
        ObjectMapper frameworkObjectMapper = new ObjectMapper(new YAMLFactory());
        frameworkObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        String configPath = "conf/config.yaml";
//        if (baseConfigPath == null) baseConfigPath = "."; // Default to current dir if parent is null

        try {
            APP_LOGGER.info("Reading main framework configuration from: " + configPath);
            appConfig = frameworkObjectMapper.readValue(new File(configPath), AppConfig.class);
            APP_LOGGER.info("Successfully read initial main framework configuration.");

            Path baseConfigPath = Path.of("conf"); // Get directory of main config
            RuntimeConfigManager rtConfigManager = new RuntimeConfigManager(appConfig, baseConfigPath);
            AutoTuner autoTuner = new AutoTuner(rtConfigManager);

            autoTuner.start();

            try (ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor()) {
                service.scheduleAtFixedRate(() -> {
                    if (appConfig.etlPipelines() == null) return;
                    for (EtlPipelineItem pipelineItem : appConfig.etlPipelines()) {
                        String pipelineName = pipelineItem.pipelineName();
                        // Check if pipeline was successfully loaded by RuntimeConfigManager
                        if (rtConfigManager.getPipelineRuntimeState(pipelineName) == null) {
                            APP_LOGGER.warning("ETL Module for Pipeline '" + pipelineName + "': Skipped as its configuration was not fully loaded.");
                            continue;
                        }

                        if (rtConfigManager.isPipelineActive(pipelineName)) {
                            if (pipelineItem.sources() != null && !pipelineItem.sources().isEmpty()) {
                                String sourceId = pipelineItem.sources().getFirst().sourceId();
                                int numThreads = rtConfigManager.getCurrentNumThreads(pipelineName, sourceId);
                                int batchSize = rtConfigManager.getCurrentBatchSize(pipelineName, sourceId);
                                APP_LOGGER.info(String.format("ETL Module for Pipeline '%s', Source '%s': Currently using numThreads=%d, batchSize=%d",
                                        pipelineName, sourceId, numThreads, batchSize));

                                // Example: Accessing detailed config
                                rtConfigManager.getValidationRules(pipelineName, sourceId).ifPresent(rules ->
                                        APP_LOGGER.fine("  Validation rules defaultMaxFieldSize: " + rules.defaultMaxFieldSize()));
                                rtConfigManager.getOutputs(pipelineName).ifPresent(outputs ->
                                        APP_LOGGER.fine("  Number of outputs defined: " + outputs.size()));
                            }
                        } else
                            APP_LOGGER.info(String.format("ETL Module for Pipeline '%s': Pipeline is currently INACTIVE.", pipelineName));

                    }
                    System.out.println("----------------------------------------------------");
                }, 10, 20, TimeUnit.SECONDS); // Check every 20 seconds


                Thread.sleep(TimeUnit.MINUTES.toMillis(5));

                APP_LOGGER.info("Shutting down ETL simulation and performance tuner...");
                autoTuner.stop();
                service.shutdownNow();
            }
            APP_LOGGER.info("Application finished.");

        } catch (IOException e) {
            APP_LOGGER.log(Level.SEVERE, "Error processing YAML configuration: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            APP_LOGGER.log(Level.INFO, "Application interrupted.");
            Thread.currentThread().interrupt();
        }
    }

    public static AppConfig getConfig() throws IOException {
        if (appConfig == null) {
            String configPath = "conf/config.yaml";
            Path baseConfigPath = Path.of("conf"); // Get directory of main config
            ObjectMapper frameworkObjectMapper = new ObjectMapper(new YAMLFactory());
            frameworkObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            appConfig = frameworkObjectMapper.readValue(new File(configPath), AppConfig.class);
        }
        return appConfig;
    }
}
