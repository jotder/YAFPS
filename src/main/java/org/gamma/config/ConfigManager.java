package org.gamma.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class ConfigManager {
    private static final Logger APP_LOGGER = Logger.getLogger(ConfigManager.class.getName());
    static AppConfig appConfig;

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT] [%4$-7s] %3$s - %5$s %6$s%n");
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter());
        handler.setLevel(Level.ALL);
        Logger rootLogger = Logger.getLogger("");
        rootLogger.addHandler(handler);
        rootLogger.setLevel(Level.INFO);
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
