package org.apache.helix.ui.health;

import com.codahale.metrics.health.HealthCheck;
import org.apache.helix.ui.util.ClientCache;

import java.util.Set;

public class ClusterConnectionHealthCheck extends HealthCheck {

    private final ClientCache clientCache;

    public ClusterConnectionHealthCheck(ClientCache clientCache) {
        this.clientCache = clientCache;
    }

    @Override
    protected Result check() throws Exception {
        Set<String> deadConnections = clientCache.getDeadConnections();
        if (!deadConnections.isEmpty()) {
            return Result.unhealthy("Dead connections to " + deadConnections);
        }
        return Result.healthy();
    }
}
