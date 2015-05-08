package io.galeb.services.metrics;


import io.galeb.core.starter.AbstractStarter;

public class Starter extends AbstractStarter {

    private Starter() {
        // main class
    }

    public static void main(String[] args) {

        loadService(MetricsService.class);

    }

}
