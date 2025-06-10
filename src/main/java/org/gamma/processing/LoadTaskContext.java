package org.gamma.processing;

import org.gamma.metrics.LoadingInfo;

import java.util.concurrent.CompletableFuture;

public record LoadTaskContext(String fileName, String tableName, CompletableFuture<LoadingInfo> future) {
    }