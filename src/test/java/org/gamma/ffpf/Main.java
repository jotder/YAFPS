package org.gamma.ffpf; // Or your preferred package for the main application class

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.gamma.config.AppConfig;
import org.gamma.config.AutoTuner;
import org.gamma.config.EtlPipelineItem;
import org.gamma.config.RuntimeConfigManager;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Main {
    private static final Logger APP_LOGGER = Logger.getLogger(Main.class.getName());

    // Static block for logger configuration (similar to ConfigManager.java)
    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT] [%4$-7s] %3$s - %5$s %6$s%n");
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter());
        handler.setLevel(Level.ALL);
        Logger rootLogger = Logger.getLogger("");

        // Remove existing console handlers to avoid duplicate output
        for (java.util.logging.Handler h : rootLogger.getHandlers()) {
            if (h instanceof ConsoleHandler) {
                rootLogger.removeHandler(h);
            }
        }
        rootLogger.addHandler(handler);
        rootLogger.setLevel(Level.INFO); // Set root logger level

        // Set levels for specific classes from your framework
        Logger.getLogger("com.gamma.config.RuntimeConfigManager").setLevel(Level.INFO);
        Logger.getLogger("com.gamma.config.AutoTuner").setLevel(Level.INFO); // Set to FINE for more tuner detail
        Logger.getLogger("com.gamma.config.MetricSimulator").setLevel(Level.FINE); // Set to FINE for metric simulation detail
        APP_LOGGER.setLevel(Level.INFO);
    }

    public static void main(String[] args) {
        ObjectMapper yamlObjectMapper = new ObjectMapper(new YAMLFactory());
        yamlObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        String mainConfigFilePath = "conf/config.yaml"; // Path to your main YAML configuration

        try {
            APP_LOGGER.info("Reading main framework configuration from: " + mainConfigFilePath);
            File configFile = new File(mainConfigFilePath);

            if (!configFile.exists() || !configFile.isFile()) {
                APP_LOGGER.severe("Main configuration file not found or is not a file: " + configFile.getAbsolutePath());
                APP_LOGGER.info("Error: Main configuration file '" + mainConfigFilePath + "' not found. Application cannot start.");
                return;
            }
            AppConfig appConfig = yamlObjectMapper.readValue(configFile, AppConfig.class);
            APP_LOGGER.info("Successfully read initial main framework configuration.");

            Path baseConfigPath = configFile.getParentFile().toPath();
            if (baseConfigPath == null) { // Fallback, though unlikely if configFile is valid
                baseConfigPath = Path.of(".");
                APP_LOGGER.warning("Could not determine base config path from main config file path. Using current directory '.' as base for pipeline-specific configs.");
            }

            RuntimeConfigManager rtConfigManager = new RuntimeConfigManager(appConfig, baseConfigPath);
            APP_LOGGER.info("RuntimeConfigManager initialized.");

            AutoTuner autoTuner;
            boolean isAnyTuningEnabled = appConfig.etlPipelines() != null && appConfig.etlPipelines().stream()
                    .anyMatch(p -> p.autoTuning() != null && Boolean.TRUE.equals(p.autoTuning().enabled()));

            if (isAnyTuningEnabled) {
                autoTuner = new AutoTuner(rtConfigManager);
                autoTuner.start();
            } else
                APP_LOGGER.info("AutoTuning is not enabled for any pipeline. AutoTuner will not be started.");

            // --- Application's main logic would go here ---
            runApplicationSimulation(appConfig, rtConfigManager);


        } catch (IOException e) {
            APP_LOGGER.log(Level.SEVERE, "Fatal Error: Could not initialize application due to YAML configuration processing error: " + e.getMessage(), e);
            APP_LOGGER.info("Fatal Error: Could not initialize application. Check logs for details.");
        } finally {
            // Ensure AutoTuner is stopped if it was started (though runApplicationSimulation handles its own shutdown)
            // This is more for a scenario where main might exit due to an error after AutoTuner started.
            // In a long-running app, shutdown is typically handled by a shutdown hook or explicit stop command.
        }
    }

    private static void runApplicationSimulation(AppConfig appConfig, RuntimeConfigManager rtConfigManager) {
        // This method simulates the application's operational loop and handles its own shutdown.
        // In a real app, this might be where your server starts, or your batch jobs are launched.
        AutoTuner autoTuner = null; // Re-check if needed or manage its instance from main
        if (appConfig.etlPipelines() != null && appConfig.etlPipelines().stream()
                .anyMatch(p -> p.autoTuning() != null && Boolean.TRUE.equals(p.autoTuning().enabled()))) {
            // If AutoTuner was started in main, you'd pass its instance or re-initialize if this is a self-contained module.
            // For simplicity, assuming AutoTuner instance from main would be managed or passed if needed here.
            // The AutoTuner itself is started/stopped in the main method's scope in this example.
        }


        try (ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "AppStatusLogger"))) {
            service.scheduleAtFixedRate(() -> {
                APP_LOGGER.info("Application Status Check:");
                if (appConfig.etlPipelines() == null || appConfig.etlPipelines().isEmpty()) {
                    APP_LOGGER.info("No ETL pipelines configured to monitor.");
                    return;
                }
                for (EtlPipelineItem pipelineItem : appConfig.etlPipelines()) {
                    String pipelineName = pipelineItem.pipelineName();

                    if (rtConfigManager.getPipelineRuntimeState(pipelineName) == null) {
                        APP_LOGGER.warning("Pipeline '" + pipelineName + "': State not available (possibly failed to load specific config).");
                        continue;
                    }

                    if (rtConfigManager.isPipelineActive(pipelineName)) {
                        if (pipelineItem.sources() != null && !pipelineItem.sources().isEmpty()) {
                            String sourceId = pipelineItem.sources().getFirst().sourceId(); // Assuming at least one source
                            int numThreads = rtConfigManager.getCurrentNumThreads(pipelineName, sourceId);
                            int batchSize = rtConfigManager.getCurrentBatchSize(pipelineName, sourceId);
                            APP_LOGGER.info(String.format("  Pipeline '%s', Source '%s': ACTIVE. Threads=%d, BatchSize=%d",
                                    pipelineName, sourceId, numThreads, batchSize));

                            // Example of accessing detailed config
                            rtConfigManager.getValidationRules(pipelineName, sourceId).ifPresent(rules ->
                                    APP_LOGGER.fine("    Validation rules defaultMaxFieldSize: " + rules.defaultMaxFieldSize()));
                            rtConfigManager.getOutputs(pipelineName).ifPresent(outputs ->
                                    APP_LOGGER.fine("    Number of outputs defined: " + outputs.size()));
                        } else {
                            APP_LOGGER.info(String.format("  Pipeline '%s': ACTIVE, but no sources listed in main config.", pipelineName));
                        }
                    } else {
                        APP_LOGGER.info(String.format("  Pipeline '%s': INACTIVE.", pipelineName));
                    }
                }
                APP_LOGGER.info("----------------------------------------------------");
            }, 5, 20, TimeUnit.SECONDS); // Start after 5s, repeat every 20s

            APP_LOGGER.info("Application simulation running. Will stop in 1 minute for this demo.");
            Thread.sleep(TimeUnit.MINUTES.toMillis(1)); // Simulate work for 1 minute

        } catch (InterruptedException e) {
            APP_LOGGER.info("Application simulation interrupted.");
            Thread.currentThread().interrupt();
        } finally {
            APP_LOGGER.info("Shutting down application simulation...");
            // Stop AutoTuner if it was started and managed within this scope or globally
            // For this example, AutoTuner's lifecycle is tied to the main method's scope.
            // If AutoTuner was started in main, its stop call should also be in main's finally block.
            // The AutoTuner instance started in main() should be stopped in main()'s finally block.
            // This simulation method is just for showing periodic work.
            APP_LOGGER.info("Application simulation finished.");
        }
    }
}