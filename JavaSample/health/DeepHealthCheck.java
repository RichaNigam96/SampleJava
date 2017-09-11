package com.amazon.dw.grasshopper.health;

import org.springframework.transaction.annotation.Transactional;

import com.amazon.coral.service.HealthCheckStrategy;

public class DeepHealthCheck implements HealthCheckStrategy {

    
    /**
     * Performs a deep health check on your service.
     *
     * You should replace this method with some simple but meaningful health
     * check. This will be invoked at startup during SanityTest to make
     * sure you have got everything configured properly before adding this
     * host to the VIP.
     *
     * Ping the database and return an up/down result.
     *
     * @return true if the database is up
     */
    @Transactional
    public boolean isHealthy()
    {
       return true;
    }

}

