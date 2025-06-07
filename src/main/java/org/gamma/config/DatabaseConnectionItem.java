package org.gamma.config;

// No special imports needed for this record beyond standard Java types.

public record DatabaseConnectionItem(
    String connectionName,
    String type,
    String dbUrl,
    String dbUser,
    String dbPass,
    Integer maxPool,
    Long idleTimeout,
    Long connTimeout,
    Long maxLifetime
) {
}
