package org.gamma.config.simulation;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.gamma.config.AppConfig;
import org.gamma.config.EtlPipelineItem;
import org.gamma.config.RuntimeConfigManager;
import org.gamma.config.AutoTuner;
import org.gamma.config.ConfigManager; // For ConfigManager.getConfig()
import org.gamma.config.MetricSimulator; // Added import for MetricSimulator from main sources

// Removed MetricSimulator inner class definition from here

public class ConfigSimulation {
    // Replicating APP_LOGGER here or making ConfigManager.APP_LOGGER public.
    // For now, creating a new one for simplicity within this test class.
    private static final Logger APP_LOGGER = Logger.getLogger(ConfigSimulation.class.getName());

    static {
        // Basic logging setup for the simulation, similar to ConfigManager's original static block
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT] [%4$-7s] %3$s - %5$s %6$s%n");
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter());
        handler.setLevel(Level.ALL); // Or a more appropriate level for test simulation
        APP_LOGGER.addHandler(handler);
        APP_LOGGER.setUseParentHandlers(false); // Avoid duplicate logging if root logger also has a handler
        APP_LOGGER.setLevel(Level.INFO);

        // Set levels for other classes if their logs are desired during simulation
        Logger.getLogger(RuntimeConfigManager.class.getName()).setLevel(Level.INFO);
        Logger.getLogger(AutoTuner.class.getName()).setLevel(Level.INFO);
        Logger.getLogger(org.gamma.config.MetricSimulator.class.getName()).setLevel(Level.FINE); // Use FQN for clarity
    }

    public static void main(String[] args) {
        ObjectMapper frameworkObjectMapper = new ObjectMapper(new YAMLFactory());
        frameworkObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        String configPath = "conf/config.yaml"; // Assuming tests run from project root or conf is in classpath

        try {
            APP_LOGGER.info("Reading main framework configuration from: " + configPath);
            // Use ConfigManager.getConfig() to get the AppConfig
            AppConfig appConfig = ConfigManager.getConfig();
            APP_LOGGER.info("Successfully read initial main framework configuration.");

            Path baseConfigPath = Path.of("conf");
            RuntimeConfigManager rtConfigManager = new RuntimeConfigManager(appConfig, baseConfigPath);
            AutoTuner autoTuner = new AutoTuner(rtConfigManager); // AutoTuner creates its own MetricSimulator

            autoTuner.start();

            try (ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor()) {
                service.scheduleAtFixedRate(() -> {
                    if (appConfig.etlPipelines() == null) return;
                    for (EtlPipelineItem pipelineItem : appConfig.etlPipelines()) {
                        String pipelineName = pipelineItem.pipelineName();
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

                                rtConfigManager.getValidationRules(pipelineName, sourceId).ifPresent(rules ->
                                        APP_LOGGER.fine("  Validation rules defaultMaxFieldSize: " + rules.defaultMaxFieldSize()));
                                rtConfigManager.getOutputs(pipelineName).ifPresent(outputs ->
                                        APP_LOGGER.fine("  Number of outputs defined: " + outputs.size()));
                            }
                        } else
                            APP_LOGGER.info(String.format("ETL Module for Pipeline '%s': Pipeline is currently INACTIVE.", pipelineName));

                    }
                    System.out.println("----------------------------------------------------");
                }, 10, 20, TimeUnit.SECONDS);


                Thread.sleep(TimeUnit.MINUTES.toMillis(1)); // Reduced from 5 min for faster test simulation if needed

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
}
// No content here, as MetricSimulator class definition was removed.
