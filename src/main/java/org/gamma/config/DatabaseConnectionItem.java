package org.gamma.config;

public record DatabaseConnectionItem(String connectionName, String type, String dbUrl, String dbUser, String dbPass,
                                     Integer maxPool, Long idleTimeout, Long connTimeout, Long maxLifetime) {
}
