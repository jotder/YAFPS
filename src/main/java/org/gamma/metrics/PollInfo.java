package org.gamma.metrics;

import java.nio.file.Path;

public record PollInfo(Path filePath, String dataSource, Status status) {
}
