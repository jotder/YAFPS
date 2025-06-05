package org.gamma.config;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

// --- PerformanceTuner (Largely Unchanged, uses RuntimeConfigManager) ---
// (PerformanceTuner class code from previous artifact can be used here,
//  as its interaction is with RuntimeConfigManager which now handles the split config internally)
public class AutoTuner implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(AutoTuner.class.getName());
    private final RuntimeConfigManager runtimeConfigManager;
    private final ScheduledExecutorService scheduler;
    private final MetricSimulator metricSimulator; // To simulate system metrics

    public AutoTuner(RuntimeConfigManager runtimeConfigManager) {
        this.runtimeConfigManager = Objects.requireNonNull(runtimeConfigManager);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "PerformanceTunerThread");
            t.setDaemon(true);
            return t;
        });
        this.metricSimulator = new MetricSimulator();
    }

    public void start() {
        int observationFrequency = runtimeConfigManager.getInitialAppConfig().etlPipelines().stream()
                .filter(p -> p.autoTuning() != null && Boolean.TRUE.equals(p.autoTuning().enabled()))
                .map(p -> p.autoTuning().observationFrequencySeconds())
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(60);

        scheduler.scheduleAtFixedRate(this, 5, observationFrequency, TimeUnit.SECONDS);
        LOGGER.info("PerformanceTuner started. Observation frequency: " + observationFrequency + "s");
    }

    public void stop() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS))
                scheduler.shutdownNow();

        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        LOGGER.info("PerformanceTuner stopped.");
    }

    @Override
    public void run() {
        try {
            LOGGER.fine("PerformanceTuner evaluating rules...");
            for (EtlPipelineItem pipelineFrameworkConfig : runtimeConfigManager.getInitialAppConfig().etlPipelines()) {
                PipelineRuntimeState pipelineState = runtimeConfigManager.getPipelineRuntimeState(pipelineFrameworkConfig.pipelineName());
                if (pipelineState == null || pipelineState.tuningConfig() == null || !Boolean.TRUE.equals(pipelineState.tuningConfig().enabled()))
                    continue;

                if (System.currentTimeMillis() < pipelineState.pipelineCooldownUntilTimestamp()) {
                    LOGGER.fine("Pipeline '" + pipelineFrameworkConfig.pipelineName() + "' is in cooldown. Skipping tuning.");
                    continue;
                }

                Map<String, Double> currentMetrics = metricSimulator.getMetricsForPipeline(pipelineFrameworkConfig.pipelineName());
                evaluateAndApplyRules(pipelineFrameworkConfig, pipelineState, currentMetrics);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error in PerformanceTuner run loop", e);
        }
    }

    private void evaluateAndApplyRules(EtlPipelineItem pipelineFrameworkConfig, PipelineRuntimeState pipelineState, Map<String, Double> currentMetrics) {
        // This method's logic remains the same as in the previous version,
        // as it operates on pipelineState and pipelineFrameworkConfig.
        // The key is that pipelineState.tuningConfig() comes from pipelineFrameworkConfig.
        for (TuningRuleConfig rule : pipelineState.tuningConfig().tuningRules()) {
            boolean ruleConditionMet = checkRuleCondition(rule, currentMetrics);
            AtomicInteger consecutiveBreaches = pipelineState.ruleConsecutiveBreaches().computeIfAbsent(rule.ruleName(), k -> new AtomicInteger(0));

            if (ruleConditionMet) {
                int breaches = consecutiveBreaches.incrementAndGet();
                LOGGER.fine(String.format("Rule '%s' for pipeline '%s' met. Consecutive breaches: %d. Required: %d",
                        rule.ruleName(), pipelineFrameworkConfig.pipelineName(), breaches, rule.consecutiveBreachesToTrigger()));

                if (breaches >= rule.consecutiveBreachesToTrigger()) {
                    LOGGER.info(String.format("Rule '%s' triggered for pipeline '%s'. Applying primary action.",
                            rule.ruleName(), pipelineFrameworkConfig.pipelineName()));
                    applyAction(pipelineFrameworkConfig, pipelineState, rule.action(), currentMetrics);
                    consecutiveBreaches.set(0);
                    if (pipelineState.tuningConfig().cooldownPeriodSeconds() != null)
                        runtimeConfigManager.setPipelineCooldown(pipelineFrameworkConfig.pipelineName(),
                                pipelineState.tuningConfig().cooldownPeriodSeconds() * 1000L);

                    return;
                } else if (rule.action() != null && rule.action().interimAction() != null) {
                    LOGGER.info(String.format("Rule '%s' for pipeline '%s' met, applying interim action (breaches %d/%d).",
                            rule.ruleName(), pipelineFrameworkConfig.pipelineName(), breaches, rule.consecutiveBreachesToTrigger()));
                    applyAction(pipelineFrameworkConfig, pipelineState, rule.action().interimAction(), currentMetrics);
                }
            } else
                consecutiveBreaches.set(0);

        }
    }

    private boolean checkRuleCondition(TuningRuleConfig rule, Map<String, Double> currentMetrics) {
        // Logic remains the same
        if (rule.conditions() != null && !rule.conditions().isEmpty()) {
            if ("ALL_MATCH".equalsIgnoreCase(rule.conditionLogic()))
                return rule.conditions().stream().allMatch(cond ->
                        checkSingleCondition(cond.metric(), cond.condition(), cond.threshold(), currentMetrics));
            else if ("ANY_MATCH".equalsIgnoreCase(rule.conditionLogic()))
                return rule.conditions().stream().anyMatch(cond ->
                        checkSingleCondition(cond.metric(), cond.condition(), cond.threshold(), currentMetrics));

            return false;
        } else {
            return checkSingleCondition(rule.metric(), rule.condition(), rule.threshold(), currentMetrics);
        }
    }

    private boolean checkSingleCondition(String metricName, String condition, Double threshold, Map<String, Double> currentMetrics) {
        // Logic remains the same
        Double metricValue = currentMetrics.get(metricName);
        if (metricValue == null || threshold == null) {
            LOGGER.warning("Metric '" + metricName + "' or threshold not available for rule evaluation.");
            return false;
        }
        return switch (condition.toLowerCase()) {
            case "greaterthan" -> metricValue > threshold;
            case "lessthan" -> metricValue < threshold;
            case "greaterthanorequal" -> metricValue >= threshold;
            case "lessthanorequal" -> metricValue <= threshold;
            case "equalto" -> metricValue.equals(threshold);
            default -> {
                LOGGER.warning("Unknown condition: " + condition);
                yield false;
            }
        };
    }

    private void applyAction(EtlPipelineItem config, PipelineRuntimeState pipelineState, RuleActionConfig actionConfig, Map<String, Double> metrics) {
        // Logic remains the same, as it uses runtimeConfigManager to update values.
        // The targetSourceId logic correctly refers to pipelineFrameworkConfig.sources()
        // which holds the SourceItem with initial numThreads/batchSize.
        if (actionConfig == null) return;
        String pipelineName = config.pipelineName();
        String sourceId = config.sources() != null && !config.sources().isEmpty() ?
                config.sources().getFirst().sourceId() : null;

        LOGGER.info(String.format("Applying action '%s' to pipeline '%s'. Details: %s",
                actionConfig.type(), pipelineName, actionConfig.params() != null ? actionConfig.params().toString() : "N/A"));

        switch (actionConfig.type().toLowerCase()) {
            case "deactivatepipelinetemporarily":
                runtimeConfigManager.updatePipelineActiveStatus(pipelineName, false, "Deactivated temporarily by rule: " + actionConfig.type());
                if (actionConfig.durationMinutes() != null) {
                    long reactivationDelay = TimeUnit.MINUTES.toMillis(actionConfig.durationMinutes());
                    new Thread(() -> {
                        try {
                            Thread.sleep(reactivationDelay);
                            runtimeConfigManager.updatePipelineActiveStatus(pipelineName, true, "Re-activated after temporary deactivation.");
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }, "PipelineReactivationThread-" + pipelineName).start();
                }
                break;
            case "deactivatepipelinepermanently":
                runtimeConfigManager.updatePipelineActiveStatus(pipelineName, false, "Deactivated permanently by rule: " + actionConfig.type() + ". Reason: " + actionConfig.reason());
                break;
            case "reduceloaddrastically":
            case "reduceload":
            case "reduceiopressure":
                if (sourceId != null && actionConfig.params() != null && pipelineState.tuningConfig() != null) { // Check pipelineState.tuningConfig
                    SourceRuntimeState sourceState = pipelineState.sourceStates().get(sourceId);
                    AdjustableParametersConfig adjParams = pipelineState.tuningConfig().adjustableParameters();
                    if (adjParams == null) {
                        LOGGER.warning("AdjustableParameters not configured for pipeline: " + pipelineName);
                        break;
                    }
                    AdjustableParameterDetailConfig numThreadsAdjConf = adjParams.numThreads();
                    AdjustableParameterDetailConfig batchSizeAdjConf = adjParams.batchSize();

                    if (actionConfig.params().targetNumThreads() != null && "min".equalsIgnoreCase(actionConfig.params().targetNumThreads()))
                        if (numThreadsAdjConf != null)
                            runtimeConfigManager.updateNumThreads(pipelineName, sourceId, numThreadsAdjConf.min());

                    if (actionConfig.params().targetBatchSizeFactor() != null && sourceState != null && batchSizeAdjConf != null) {
                        int currentBatch = sourceState.currentBatchSize().get();
                        int newBatch = (int) Math.max(batchSizeAdjConf.min(), currentBatch * actionConfig.params().targetBatchSizeFactor());
                        runtimeConfigManager.updateBatchSize(pipelineName, sourceId, newBatch);
                    } else if (actionConfig.params().batchSizeDecrementFactor() != null && sourceState != null && batchSizeAdjConf != null) {
                        int currentBatch = sourceState.currentBatchSize().get();
                        int newBatch = (int) Math.max(batchSizeAdjConf.min(), currentBatch * actionConfig.params().batchSizeDecrementFactor());
                        runtimeConfigManager.updateBatchSize(pipelineName, sourceId, newBatch);
                    }
                }
                break;
            case "increaseload":
                if (sourceId != null && actionConfig.params() != null && pipelineState.tuningConfig() != null) { // Check pipelineState.tuningConfig
                    SourceRuntimeState sourceState = pipelineState.sourceStates().get(sourceId);
                    AdjustableParametersConfig adjParams = pipelineState.tuningConfig().adjustableParameters();
                    if (adjParams == null) {
                        LOGGER.warning("AdjustableParameters not configured for pipeline: " + pipelineName);
                        break;
                    }
                    AdjustableParameterDetailConfig numThreadsAdjConf = adjParams.numThreads();
                    AdjustableParameterDetailConfig batchSizeAdjConf = adjParams.batchSize();

                    if (actionConfig.params().numThreadsIncrement() != null && sourceState != null && numThreadsAdjConf != null) {
                        int currentThreads = sourceState.currentNumThreads().get();
                        int newThreads = Math.min(numThreadsAdjConf.max(), currentThreads + actionConfig.params().numThreadsIncrement());
                        runtimeConfigManager.updateNumThreads(pipelineName, sourceId, newThreads);
                    }
                    if (actionConfig.params().batchSizeIncrementFactor() != null && sourceState != null && batchSizeAdjConf != null) {
                        int currentBatch = sourceState.currentBatchSize().get();
                        int newBatch = (int) Math.min(batchSizeAdjConf.max(), currentBatch * actionConfig.params().batchSizeIncrementFactor());
                        runtimeConfigManager.updateBatchSize(pipelineName, sourceId, newBatch);
                    }
                }
                break;
            default:
                LOGGER.warning("Unknown action type: " + actionConfig.type() + " for pipeline: " + pipelineName);
        }
    }
}
