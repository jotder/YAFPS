package org.gamma.metrics;

import java.nio.file.Path;

public record RouteInfo(String dataSource, Path srcFile, Path outFile, long count) {
}