package io.galeb.services.metrics;

import java.io.Serializable;

import io.galeb.core.controller.EntityController.Action;
import io.galeb.core.eventbus.Event;
import io.galeb.core.json.JsonObject;
import io.galeb.core.model.Backend;
import io.galeb.core.model.BackendPool;
import io.galeb.core.model.Entity;
import io.galeb.core.model.Rule;
import io.galeb.core.queue.QueueListener;
import io.galeb.core.queue.QueueManager;
import io.galeb.core.services.AbstractService;
import io.galeb.core.statsd.StatsdClient;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

public class Metrics extends AbstractService implements QueueListener {

    public static final String PREFIX = Metrics.class.getPackage().getName();

    public static final String STATSD_HOST = "statsdHost";
    public static final String STATSD_PORT = "statsdPort";
    public static final String STATSD_PREFIX = "statsdPrefix";

    static {
        if (System.getProperty(PREFIX + "." + STATSD_HOST)==null) {
            System.setProperty(PREFIX + "." + STATSD_HOST, "127.0.0.1");
        }
        if (System.getProperty(PREFIX + "." + STATSD_PORT)==null) {
            System.setProperty(PREFIX + "." + STATSD_PORT, "8125");
        }
        if (System.getProperty(PREFIX + "." + STATSD_PREFIX)==null) {
            System.setProperty(PREFIX + "." + STATSD_PREFIX, "galeb");
        }
    }


    @Inject
    private StatsdClient client;

    private QueueManager queueManager = QueueManager.NULL;

    @PostConstruct
    protected void init() {
        super.prelaunch();

        startGateway();

        logger.debug(String.format("%s ready", toString()));
    }

    private void startGateway() {
        queueManager = eventbus.getQueueManager();
        queueManager.register(this);
        final String host = System.getProperty(PREFIX + "." + STATSD_HOST);
        final Integer port = Integer.valueOf(System.getProperty(PREFIX + "." + STATSD_HOST));
        final String prefix = System.getProperty(PREFIX + "." + STATSD_HOST);
        client.server(host).port(port).prefix(prefix);
    }

    @Override
    public void onEvent(Serializable data) {
        if (data instanceof io.galeb.core.model.Metrics) {
            final io.galeb.core.model.Metrics metrics = (io.galeb.core.model.Metrics) data;
            final Long requestTimeAvg = (Long) metrics.getProperties().get(io.galeb.core.model.Metrics.PROP_REQUESTTIME_AVG);
            sendRequestTime(metrics, requestTimeAvg);
            for (final String propertyName: metrics.getProperties().keySet()) {
                if (propertyName.startsWith(io.galeb.core.model.Metrics.PROP_HTTPCODE_PREFIX)) {
                    final Integer statusCodeCount = (Integer) metrics.getProperties().get(propertyName);
                    sendStatusCodeCount(metrics, propertyName, statusCodeCount);
                }
            }
        }
    }

    private String extractMetricKeyPrefix(io.galeb.core.model.Metrics metrics) {
        final String virtualHostId = metrics.getParentId();
        final String backendId = metrics.getId();

        return extractMetricKeyPrefix(virtualHostId, backendId);
    }

    private String extractMetricKeyPrefix(String virtualHostId, String backendId) {
        virtualHostId = virtualHostId.replace(".", "_");
        backendId = backendId.replace(".", "_");
        final String clusterId = eventbus.getClusterId().replace(".", "_");

        return clusterId + "." + virtualHostId + "." + backendId;
    }

    private void sendStatusCodeCount(io.galeb.core.model.Metrics metrics, String httpCodeName, Integer statusCodeCount) {
        final String metricKeyPrefix = extractMetricKeyPrefix(metrics);
        final String metricName = metricKeyPrefix + "." + httpCodeName;
        client.count(metricName, statusCodeCount);
    }

    private void sendRequestTime(io.galeb.core.model.Metrics metrics, Long requestTimeAvg) {
        final String metricKeyPrefix = extractMetricKeyPrefix(metrics);
        final String metricName = metricKeyPrefix + "." + io.galeb.core.model.Metrics.PROP_REQUESTTIME;
        client.timing(metricName, requestTimeAvg);
    }

    @Override
    public void onEvent(Event event) throws RuntimeException {
        super.onEvent(event);

        final JsonObject json = event.getData();

        final Entity entity = (Entity) json.instanceOf(Entity.class);
        final Object eventType = event.getType();

        if (entity instanceof Backend && eventType instanceof Action && eventType == Action.CHANGE) {
            final BackendPool backendPool = (BackendPool) farm.getBackend(entity.getParentId());
            String virtualhostId = "";

            for (final Rule rule: farm.getRules()){
                if (rule.getProperties().get(Rule.PROP_TARGET_ID).equals(backendPool.getId())) {
                    virtualhostId = rule.getParentId();
                    break;
                }
            }
            final String metricKeyPrefix = extractMetricKeyPrefix(virtualhostId, entity.getId());
            final String metricName = metricKeyPrefix + "." + Backend.PROP_ACTIVECONN;
            client.count(metricName, ((Backend) entity).getConnections());
        }
    }

}
