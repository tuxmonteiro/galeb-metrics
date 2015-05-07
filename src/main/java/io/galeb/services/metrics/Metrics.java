package io.galeb.services.metrics;

import io.galeb.core.services.AbstractService;

import javax.annotation.PostConstruct;

public class Metrics extends AbstractService {

    @PostConstruct
    protected void init() {
        super.prelaunch();

        logger.debug(String.format("%s ready", toString()));
    }

}
