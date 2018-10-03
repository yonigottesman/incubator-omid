/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.tso;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import org.apache.omid.metrics.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.omid.metrics.MetricsUtils.name;
import java.util.concurrent.TimeUnit;

@NotThreadSafe
public class MonitoringContextImpl implements MonitoringContext{

    private static final Logger LOG = LoggerFactory.getLogger(MonitoringContextImpl.class);

    private volatile boolean flag;
    private Map<String, Long> elapsedTimeMsMap = new ConcurrentHashMap<>();
    private Map<String, Stopwatch> timers = new ConcurrentHashMap<>();
    private MetricsRegistry metrics;

    public MonitoringContextImpl(MetricsRegistry metrics) {
        this.metrics = metrics;
    }

    public void timerStart(String name) {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        timers.put(name, stopwatch);
    }

    public void timerStop(String name) {
        if (flag) {
            LOG.warn("timerStop({}) called after publish. Measurement was ignored. {}", name, Throwables.getStackTraceAsString(new Exception()));
            return;
        }
        Stopwatch activeStopwatch = timers.get(name);
        if (activeStopwatch == null) {
            throw new IllegalStateException(
                    String.format("There is no %s timer in the %s monitoring context.", name, this));
        }
        activeStopwatch.stop();
        elapsedTimeMsMap.put(name, activeStopwatch.elapsedTime(TimeUnit.NANOSECONDS));
        timers.remove(name);
    }

    public void publish() {
        flag = true;
        for (Map.Entry<String, Long> entry : elapsedTimeMsMap.entrySet()) {
            metrics.timer(name("tso", entry.getKey())).update(entry.getValue());
        }
    }

}
