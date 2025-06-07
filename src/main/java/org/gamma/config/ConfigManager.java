package org.gamma.config;

// import com.fasterxml.jackson.annotation.JsonProperty; // No longer needed directly
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
// import java.util.List; // AppConfig might use it, but ConfigManager itself doesn't directly. Will be caught by compiler if needed by AppConfig's direct usage here.
// import java.util.Map;
// import java.util.concurrent.ConcurrentHashMap;
// import java.util.concurrent.atomic.AtomicBoolean;
// import java.util.concurrent.atomic.AtomicInteger;
// Executors, ScheduledExecutorService, TimeUnit were only for the main method, so they remain removed.
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

// DatabaseConnectionItem moved to its own file

// PipelineDetailsConfig moved to its own file

// SourceDetailsItem moved to its own file

// FieldValidationConfigItem moved to its own file

// ValidationExpressionItem moved to its own file

// TransformationItem moved to its own file

// AggregationDetailItem moved to its own file

// AdjustableParametersConfig moved to its own file

// AdjustableParameterDetailConfig moved to its own file

// TuningRuleConfig moved to its own file

// SingleConditionConfig moved to its own file

// RuleActionConfig moved to its own file

// RuleActionParamsConfig moved to its own file

// SourceRuntimeState and PipelineRuntimeState moved to RuntimeConfigManager.java

// MetricSimulator class has been moved to src/main/java/org/gamma/config/MetricSimulator.java (corrected path)

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
        // Logger.getLogger(RuntimeConfigManager.class.getName()).setLevel(Level.INFO); // RuntimeConfigManager related logic moved
        // Logger.getLogger(AutoTuner.class.getName()).setLevel(Level.INFO); // AutoTuner related logic moved
        // Logger.getLogger(MetricSimulator.class.getName()).setLevel(Level.FINE); // MetricSimulator moved
    }

    static AppConfig appConfig;

    // main method has been moved to src/test/java/org/gamma/config/simulation/ConfigSimulation.java

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
