/*
 * Copyright (c) 2014-2015 Globo.com - ATeam
 * All rights reserved.
 *
 * This source is subject to the Apache License, Version 2.0.
 * Please see the LICENSE file for more information.
 *
 * Authors: See AUTHORS file
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.galeb.services.metrics;

import io.galeb.core.controller.EntityController.Action;
import io.galeb.core.eventbus.Event;
import io.galeb.core.json.JsonObject;
import io.galeb.core.model.Backend;
import io.galeb.core.model.BackendPool;
import io.galeb.core.model.Entity;
import io.galeb.core.model.Metrics;
import io.galeb.core.model.Rule;
import io.galeb.core.model.collections.BackendPoolCollection;
import io.galeb.core.queue.QueueListener;
import io.galeb.core.queue.QueueManager;
import io.galeb.core.services.AbstractService;
import io.galeb.core.statsd.StatsdClient;

import java.io.Serializable;
import java.util.List;

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
        backendId = backendId.replaceAll("http://", "").replaceAll("[.:]", "_");
        return virtualHostId + "." + backendId;
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
        final String backendEntityName = Backend.class.getSimpleName().toLowerCase();
        Action action = Action.UNKNOWN;

        if (event.getType() instanceof Action) {
            action = (Action)event.getType();
        }

        if (entity.getEntityType().equals(backendEntityName) && action == Action.CHANGE) {

            final BackendPoolCollection backendPoolsCollections = (BackendPoolCollection) farm.getCollection(BackendPool.class);
            farm.getCollection(Backend.class).stream()
                .filter(backend -> backend.getId().equals(entity.getId()))
                .forEach(backend -> {

                    final List<Entity> backendPools = backendPoolsCollections.getListByID(backend.getParentId());

                    if (backendPools.isEmpty()) {
                        logger.error(backend.getParentId() + " NOT FOUND");
                        return;
                    }

                    final BackendPool backendPool = (BackendPool) backendPools.get(0);
                    String virtualhostId = "";
                    String lastVirtualhost = "";

                    for (final Entity rule: farm.getCollection(Rule.class)){
                        if (rule.getProperty(Rule.PROP_TARGET_ID).equals(backendPool.getId())) {
                            virtualhostId = rule.getParentId();
                            if (virtualhostId != null && !virtualhostId.equals(lastVirtualhost)) {
                                lastVirtualhost = virtualhostId;
                                final String metricKeyPrefix = extractMetricKeyPrefix(virtualhostId, backend.getId());
                                final String metricName = metricKeyPrefix + "." + Backend.PROP_ACTIVECONN;
                                client.gauge(metricName, Integer.valueOf(((Backend) backend).getConnections()).doubleValue());
                            }
                        }
                    }
                });
        }
    }

}
