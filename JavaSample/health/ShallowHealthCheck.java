package com.amazon.dw.grasshopper.health;

import com.amazon.coral.service.HealthCheckStrategy;

public class ShallowHealthCheck implements HealthCheckStrategy {

    /**
     * Performs a shallow health check on your service.
     *
     * Performs quick and local checks on the health of
     * this service node.
     *
     * @return true if the service node is healthy
     */
    public boolean isHealthy()
    {
        // Helpful things to put here:
        // - Checks that the filesystem is working how you expect
        // - Checks that any local caches are working
        // - Checks that Odin is working properly
        //
        // DO NOT: add checks that depend on a remote resource
        // (like a DB or other service), since if they fail,
        // this check will fail for every host, and all hosts in
        // your VIP will fail health check and be taken out of
        // service.
        return true;
    }

}

