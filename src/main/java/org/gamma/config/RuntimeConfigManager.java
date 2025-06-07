package org.gamma.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean; // Added for moved records
import java.util.concurrent.atomic.AtomicInteger; // Added for moved records
import java.util.logging.Level;
import java.util.logging.Logger;

// --- RuntimeConfigManager (Adapted for split config) ---
public class RuntimeConfigManager {

    // --- Moved Runtime State Records ---
    public static record SourceRuntimeState(String sourceId, AtomicInteger currentNumThreads, AtomicInteger currentBatchSize,
                                            AtomicBoolean currentActive) {
        public SourceRuntimeState(SourceItem sourceConfig) { // Takes simplified SourceItem
            this(sourceConfig.sourceId(), new AtomicInteger(sourceConfig.numThreads() != null ? sourceConfig.numThreads() : 1),
                    new AtomicInteger(sourceConfig.batchSize() != null ? sourceConfig.batchSize() : 10),
                    new AtomicBoolean(sourceConfig.active()));
        }
    }

    public static record PipelineRuntimeState(String pipelineName, AtomicBoolean currentActive,
                                Map<String, SourceRuntimeState> sourceStates, AutoTuningConfig tuningConfig,
                                PipelineDetailsConfig detailsConfig, // These are top-level records in this package
                                Map<String, AtomicInteger> ruleConsecutiveBreaches, long pipelineCooldownUntilTimestamp) {
        public PipelineRuntimeState(EtlPipelineItem pipelineConfig, PipelineDetailsConfig detailsConfig) {
            this(pipelineConfig.pipelineName(), new AtomicBoolean(pipelineConfig.active()),
                    new ConcurrentHashMap<>(), pipelineConfig.autoTuning(), detailsConfig,
                    new ConcurrentHashMap<>(), 0L);
            if (pipelineConfig.sources() != null) {
                for (SourceItem sourceCoreConfig : pipelineConfig.sources())
                    this.sourceStates.put(sourceCoreConfig.sourceId(), new SourceRuntimeState(sourceCoreConfig));
            }
            if (this.tuningConfig != null && this.tuningConfig.tuningRules() != null) {
                for (TuningRuleConfig rule : this.tuningConfig.tuningRules()) // TuningRuleConfig is a top-level record
                    this.ruleConsecutiveBreaches.put(rule.ruleName(), new AtomicInteger(0));
            }
        }
    }
    // --- End of Moved Runtime State Records ---

    private static final Logger LOGGER = Logger.getLogger(RuntimeConfigManager.class.getName());
    private final AppConfig initialAppConfig;
    private final Map<String, PipelineRuntimeState> pipelineStates; // Key: pipelineName

    public RuntimeConfigManager(AppConfig initialAppConfig, Path configPath) throws IOException {
        this.initialAppConfig = Objects.requireNonNull(initialAppConfig);
        this.pipelineStates = new ConcurrentHashMap<>();
        // For loading pipeline-specific files
        ObjectMapper yamlObjectMapper = new ObjectMapper(new YAMLFactory());
        yamlObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        if (initialAppConfig.etlPipelines() != null) {
            for (EtlPipelineItem config : initialAppConfig.etlPipelines()) {
                String pipelineName = config.pipelineName();
                File specificConfigFile = configPath.resolve(config.pipelineSpecificConfigFile()).toFile();
                if (!specificConfigFile.exists()) {
                    LOGGER.severe("Pipeline specific config file not found for '" + pipelineName + "': " + specificConfigFile.getAbsolutePath());
                    // Decide on error handling: throw exception, or skip pipeline?
                    // For now, log and skip initializing full runtime state for this pipeline.
                    // A basic state might still be needed if some operations don't depend on details.
                    continue;
                }

                LOGGER.info("Loading specific config for pipeline '" + pipelineName + "' from: " + specificConfigFile.getAbsolutePath());
                PipelineDetailsConfig detailsConfig = yamlObjectMapper.readValue(specificConfigFile, PipelineDetailsConfig.class);

                if (!pipelineName.equals(detailsConfig.pipelineName())) {
                    LOGGER.severe("Mismatch in pipelineName between main config ('" + pipelineName
                                  + "') and specific config ('" + detailsConfig.pipelineName() + "') in file: "
                                  + specificConfigFile.getAbsolutePath());
                    continue; // Skip this pipeline
                }

                this.pipelineStates.put(pipelineName, new PipelineRuntimeState(config, detailsConfig));
                LOGGER.info("Initialized runtime state for pipeline: " + pipelineName + " with its specific details.");
            }
        }
    }

    public AppConfig getInitialAppConfig() {
        return initialAppConfig;
    }

    public PipelineRuntimeState getPipelineRuntimeState(String pipelineName) {
        return pipelineStates.get(pipelineName);
    }

    // --- Getters for ETL Modules ---
    public boolean isPipelineActive(String pipelineName) {
        PipelineRuntimeState state = pipelineStates.get(pipelineName);
        return state != null && state.currentActive().get();
    }

    public int getCurrentNumThreads(String pipelineName, String sourceId) {
        PipelineRuntimeState pState = pipelineStates.get(pipelineName);
        if (pState != null) {
            SourceRuntimeState sState = pState.sourceStates().get(sourceId);
            if (sState != null) return sState.currentNumThreads().get();
        }
        // Fallback to initial config if runtime state not fully initialized or source not found in runtime
        // This fallback logic might need refinement based on how strictly you want to enforce full initialization.
        return initialAppConfig.etlPipelines().stream()
                .filter(p -> p.pipelineName().equals(pipelineName))
                .findFirst()
                .flatMap(p -> p.sources().stream().filter(s -> s.sourceId().equals(sourceId)).findFirst())
                .map(s -> s.numThreads() != null ? s.numThreads() : 1)
                .orElse(1);
    }

    public int getCurrentBatchSize(String pipelineName, String sourceId) {
        PipelineRuntimeState pState = pipelineStates.get(pipelineName);
        if (pState != null) {
            SourceRuntimeState sState = pState.sourceStates().get(sourceId);
            if (sState != null) return sState.currentBatchSize().get();
        }
        return initialAppConfig.etlPipelines().stream()
                .filter(p -> p.pipelineName().equals(pipelineName))
                .findFirst()
                .flatMap(p -> p.sources().stream().filter(s -> s.sourceId().equals(sourceId)).findFirst())
                .map(s -> s.batchSize() != null ? s.batchSize() : 10)
                .orElse(10);
    }

    // --- Getters for detailed configurations (from pipeline-specific files) ---
    public Optional<ValidationRulesItem> getValidationRules(String pipelineName, String sourceId) {
        PipelineRuntimeState pState = pipelineStates.get(pipelineName);
        if (pState != null && pState.detailsConfig() != null && pState.detailsConfig().sourcesDetails() != null) {
            return pState.detailsConfig().sourcesDetails().stream()
                    .filter(sd -> sd.sourceId().equals(sourceId))
                    .map(SourceDetailsItem::validationRules)
                    .findFirst();
        }
        return Optional.empty();
    }

    public Optional<List<OutputItem>> getOutputs(String pipelineName) {
        PipelineRuntimeState pState = pipelineStates.get(pipelineName);
        if (pState != null && pState.detailsConfig() != null) {
            return Optional.ofNullable(pState.detailsConfig().outputs());
        }
        return Optional.empty();
    }

    public Optional<String> getSourceFields(String pipelineName, String sourceId) {
        PipelineRuntimeState pState = pipelineStates.get(pipelineName);
        if (pState != null && pState.detailsConfig() != null && pState.detailsConfig().sourcesDetails() != null) {
            return pState.detailsConfig().sourcesDetails().stream()
                    .filter(sd -> sd.sourceId().equals(sourceId))
                    .map(SourceDetailsItem::sourceFields)
                    .findFirst();
        }
        return Optional.empty();
    }


    // --- Updaters for PerformanceTuner (largely unchanged, operate on runtime state) ---
    public synchronized void updatePipelineActiveStatus(String pipelineName, boolean newActiveStatus, String reason) {
        PipelineRuntimeState state = pipelineStates.get(pipelineName);
        if (state != null) {
            boolean oldStatus = state.currentActive().getAndSet(newActiveStatus);
            if (oldStatus != newActiveStatus) {
                LOGGER.log(Level.INFO, String.format("Pipeline '%s' active status changed from %b to %b. Reason: %s",
                        pipelineName, oldStatus, newActiveStatus, reason));
            }
        } else {
            LOGGER.warning("Attempted to update active status for unknown/unloaded pipeline: " + pipelineName);
        }
    }

    public synchronized void setPipelineCooldown(String pipelineName, long cooldownMillis) {
        PipelineRuntimeState state = pipelineStates.get(pipelineName);
        if (state != null) {
            long newCooldownUntil = System.currentTimeMillis() + cooldownMillis;
            // Create a new state object to update the final field
            PipelineRuntimeState newState = new PipelineRuntimeState(state.pipelineName(), state.currentActive(), state.sourceStates(),
                    state.tuningConfig(), state.detailsConfig(), state.ruleConsecutiveBreaches(), newCooldownUntil);
            pipelineStates.put(pipelineName, newState);
            LOGGER.fine("Pipeline '" + pipelineName + "' cooldown set until: " + newCooldownUntil);
        }
    }

    public synchronized void updateNumThreads(String pipelineName, String sourceId, int newNumThreads) {
        PipelineRuntimeState pState = pipelineStates.get(pipelineName);
        if (pState != null && pState.tuningConfig() != null && pState.tuningConfig().adjustableParameters() != null) {
            AdjustableParameterDetailConfig numThreadsConfig = pState.tuningConfig().adjustableParameters().numThreads();
            if (numThreadsConfig == null) {
                LOGGER.warning("NumThreads not configured as adjustable for " + pipelineName + "/" + sourceId);
                return;
            }
            int clampedNumThreads = Math.max(numThreadsConfig.min(), Math.min(numThreadsConfig.max(), newNumThreads));
            SourceRuntimeState sState = pState.sourceStates().get(sourceId);
            if (sState != null) {
                int oldNumThreads = sState.currentNumThreads().getAndSet(clampedNumThreads);
                if (oldNumThreads != clampedNumThreads)
                    LOGGER.log(Level.INFO, String.format("Pipeline '%s', Source '%s': numThreads changed from %d to %d.",
                            pipelineName, sourceId, oldNumThreads, clampedNumThreads));

            } else
                LOGGER.warning("Attempted to update numThreads for unknown source: " + sourceId + " in pipeline: " + pipelineName);

        } else
            LOGGER.warning("Attempted to update numThreads for pipeline without tuning config: " + pipelineName);

    }

    public synchronized void updateBatchSize(String pipelineName, String sourceId, int newBatchSize) {
        PipelineRuntimeState pState = pipelineStates.get(pipelineName);
        if (pState != null && pState.tuningConfig() != null && pState.tuningConfig().adjustableParameters() != null) {
            AdjustableParameterDetailConfig batchSizeConfig = pState.tuningConfig().adjustableParameters().batchSize();
            if (batchSizeConfig == null) {
                LOGGER.warning("BatchSize not configured as adjustable for " + pipelineName + "/" + sourceId);
                return;
            }
            int clampedBatchSize = Math.max(batchSizeConfig.min(), Math.min(batchSizeConfig.max(), newBatchSize));
            SourceRuntimeState sState = pState.sourceStates().get(sourceId);
            if (sState != null) {
                int oldBatchSize = sState.currentBatchSize().getAndSet(clampedBatchSize);
                if (oldBatchSize != clampedBatchSize)
                    LOGGER.log(Level.INFO, String.format("Pipeline '%s', Source '%s': batchSize changed from %d to %d.",
                            pipelineName, sourceId, oldBatchSize, clampedBatchSize));

            } else
                LOGGER.warning("Attempted to update batchSize for unknown source: " + sourceId + " in pipeline: " + pipelineName);

        } else
            LOGGER.warning("Attempted to update batchSize for pipeline without tuning config: " + pipelineName);

    }
}
