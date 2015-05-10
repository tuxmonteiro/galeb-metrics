package io.galeb.services.metrics;

import io.galeb.core.controller.EntityController.Action;
import io.galeb.core.eventbus.Event;
import io.galeb.core.json.JsonObject;
import io.galeb.core.model.Backend;
import io.galeb.core.model.BackendPool;
import io.galeb.core.model.Entity;
import io.galeb.core.model.Metrics;
import io.galeb.core.model.Rule;
import io.galeb.core.queue.QueueListener;
import io.galeb.core.queue.QueueManager;
import io.galeb.core.services.AbstractService;
import io.galeb.core.statsd.StatsdClient;

import java.io.Serializable;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

public class MetricsService extends AbstractService implements QueueListener {

    public static final String PREFIX = MetricsService.class.getPackage().getName();

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
    public void init() {
        super.prelaunch();

        startGateway();

        logger.debug(String.format("%s ready", toString()));
    }

    private void startGateway() {
        queueManager = eventbus.getQueueManager();
        queueManager.register(this);
        final String host = System.getProperty(PREFIX + "." + STATSD_HOST);
        final Integer port = Integer.valueOf(System.getProperty(PREFIX + "." + STATSD_PORT));
        final String prefix = System.getProperty(PREFIX + "." + STATSD_PREFIX);
        client.server(host).port(port).prefix(prefix);
    }

    @Override
    public void onEvent(Serializable data) {
        if (data instanceof Metrics) {
            final Metrics metrics = (Metrics) data;
            final Long requestTimeAvg = (Long) metrics.getProperty(Metrics.PROP_REQUESTTIME_AVG);
            sendRequestTime(metrics, requestTimeAvg);
            for (final String propertyName: metrics.getProperties().keySet()) {
                if (propertyName.startsWith(Metrics.PROP_HTTPCODE_PREFIX)) {
                    final Integer statusCodeCount = (Integer) metrics.getProperty(propertyName);
                    sendStatusCodeCount(metrics, propertyName, statusCodeCount);
                }
            }
        }
    }

    private String extractMetricKeyPrefix(Metrics metrics) {
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

    private void sendStatusCodeCount(Metrics metrics, String httpCodeName, Integer statusCodeCount) {
        final String metricKeyPrefix = extractMetricKeyPrefix(metrics);
        final String metricName = metricKeyPrefix + "." + httpCodeName;
        client.count(metricName, statusCodeCount);
    }

    private void sendRequestTime(Metrics metrics, Long requestTimeAvg) {
        final String metricKeyPrefix = extractMetricKeyPrefix(metrics);
        final String metricName = metricKeyPrefix + "." + Metrics.PROP_REQUESTTIME;
        client.timing(metricName, requestTimeAvg);
    }

    @Override
    public void onEvent(Event event) throws RuntimeException {
        super.onEvent(event);

        final JsonObject json = event.getData();

        final Entity entity = (Entity) json.instanceOf(Entity.class);
        final Object eventType = event.getType();

        if (entity instanceof Backend && eventType instanceof Action && eventType == Action.CHANGE) {
            final BackendPool backendPool = (BackendPool) farm.getBackends(entity.getParentId());
            String virtualhostId = "";

            for (final Rule rule: farm.getRules()){
                if (rule.getProperty(Rule.PROP_TARGET_ID).equals(backendPool.getId())) {
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
