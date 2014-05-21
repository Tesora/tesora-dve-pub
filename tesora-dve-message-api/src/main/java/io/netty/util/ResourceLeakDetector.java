// OS_STATUS: public
/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.util;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Netty ResourceLeakDetector to provide more aggressive and verbose leak
 * detection.
 */
public final class ResourceLeakDetector<T> {

	public static final String SYSTEM_ENABLE_LEAK_DETECTION = "com.tesora.dve.netty.leakDetection.enabled";
	public static final String SYSTEM_LEAK_DETECTION_INTERVAL = "com.tesora.dve.netty.leakDetection.interval";
	public static final String SYSTEM_REPORT_ALL = "com.tesora.dve.netty.leakDetection.reportAll";

	private static int DEFAULT_SAMPLING_INTERVAL = SystemPropertyUtil.getInt(SYSTEM_LEAK_DETECTION_INTERVAL, 113);
	private static boolean reportAll = SystemPropertyUtil.getBoolean(SYSTEM_REPORT_ALL, true);
	private static boolean enabled = SystemPropertyUtil.getBoolean(SYSTEM_ENABLE_LEAK_DETECTION, false);

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ResourceLeakDetector.class);
    
    public static boolean isEnabled() {
        return enabled;
    }

    /** the linked list of active resources */
    private final DefaultResourceLeak head = new DefaultResourceLeak(null);
    private final DefaultResourceLeak tail = new DefaultResourceLeak(null);

    private final ReferenceQueue<Object> refQueue = new ReferenceQueue<Object>();
    private final ConcurrentMap<Exception, Boolean> reportedLeaks = PlatformDependent.newConcurrentHashMap();

    private final String resourceType;
    private final int samplingInterval;
    private final long maxActive;
    private long active;
    private long found = 0;
    private final AtomicBoolean loggedTooManyActive = new AtomicBoolean();

    private long leakCheckCnt;
    
    private static HashMap<String, ResourceLeakDetector<?>> detectors = new HashMap<String, ResourceLeakDetector<?>>();
    
    public static ResourceLeakDetector<?> getDetector(Class<?> resourceType) {
    	return detectors.get(resourceType.getSimpleName());
    }

    public ResourceLeakDetector(Class<?> resourceType) {
        this(resourceType.getSimpleName());
    }

    public ResourceLeakDetector(String resourceType) {
        this(resourceType, DEFAULT_SAMPLING_INTERVAL, Long.MAX_VALUE);
    }

    public ResourceLeakDetector(Class<?> resourceType, int samplingInterval, long maxActive) {
        this(resourceType.getSimpleName(), samplingInterval, maxActive);
    }

    public ResourceLeakDetector(String resourceType, int samplingInterval, long maxActive) {
        if (resourceType == null) {
            throw new NullPointerException("resourceType");
        }
        if (samplingInterval <= 0) {
            throw new IllegalArgumentException("samplingInterval: " + samplingInterval + " (expected: 1+)");
        }
        if (maxActive <= 0) {
            throw new IllegalArgumentException("maxActive: " + maxActive + " (expected: 1+)");
        }
        
		logger.warn("DVE netty ResourceLeakDetector<" + resourceType + "> enabled=" + enabled + " samplingInterval=" + samplingInterval + " reportAll=" + reportAll);

        this.resourceType = resourceType;
        this.samplingInterval = samplingInterval;
        this.maxActive = maxActive;

        head.next = tail;
        tail.prev = head;
        
        detectors.put(resourceType,  this);
    }

    /**
     * Creates a new {@link ResourceLeak} which is expected to be closed via {@link ResourceLeak#close()} when the
     * related resource is deallocated.
     *
     * @return the {@link ResourceLeak} or {@code null}
     */
    public ResourceLeak open(T obj) {
        if (!enabled || leakCheckCnt ++ % samplingInterval != 0) {
            return null;
        }

        reportLeak();

        return new DefaultResourceLeak(obj);
    }
    
    public long getLeakCount() {
    	return found;
    }

    private void reportLeak() {
        if (!logger.isWarnEnabled()) {
            for (;;) {
                @SuppressWarnings("unchecked")
                DefaultResourceLeak ref = (DefaultResourceLeak) refQueue.poll();
                if (ref == null) {
                    break;
                }
                ref.close();
            }
            return;
        }

        // Report too many instances.
        if (active * samplingInterval > maxActive && loggedTooManyActive.compareAndSet(false, true)) {
            logger.warn(
                    "LEAK: You are creating too many " + resourceType + " instances.  " +
                    resourceType + " is a shared resource that must be reused across the JVM," +
                    "so that only a few instances are created.");
        }

        // Detect and report previous leaks.
        for (;;) {
            @SuppressWarnings("unchecked")
            DefaultResourceLeak ref = (DefaultResourceLeak) refQueue.poll();
            if (ref == null) {
                break;
            }

            ref.clear();

            if (!ref.doClose()) { // don't log
                continue;
            }

            if (reportAll || reportedLeaks.putIfAbsent(ref.exception, Boolean.TRUE) == null) {
                logger.warn("LEAK: " + resourceType + "(" + ref.objDesc + "), found=" + found, ref.exception);
            }
            found ++;
        }
    }

    private final class DefaultResourceLeak extends PhantomReference<Object> implements ResourceLeak {

        private final ResourceLeakException exception;
        private final AtomicBoolean freed;
        private DefaultResourceLeak prev;
        private DefaultResourceLeak next;

        private final String objId;
        private final String objDesc;
        
        public DefaultResourceLeak(Object referent) {
            super(referent, referent != null? refQueue : null);

            if (referent != null) {
				objId = referent.getClass().getName() + '@' + Integer.toHexString(System.identityHashCode(referent));
				objDesc = referent.toString();
            	if (logger.isDebugEnabled()) {
            		logger.debug("allocated: " + objId + " (" + referent.toString() + ")");
            	}

            	exception = new ResourceLeakException(objId);
                
                synchronized (head) {
                    prev = head;
                    next = head.next;
                    head.next.prev = this;
                    head.next = this;
                    active ++;
                }
                freed = new AtomicBoolean();
            } else {
                exception = null;
                freed = new AtomicBoolean(true);
                objId = null;
                objDesc = null;
            }
        }

        @Override
        public boolean close() {
        	if (logger.isDebugEnabled())
        		logger.debug("released: " + objId);
        	return doClose();
        }
        
        private boolean doClose() {
            if (freed.compareAndSet(false, true)) {
                synchronized (head) {
                    active --;
                    prev.next = next;
                    next.prev = prev;
                    prev = null;
                    next = null;
                }
                return true;
            }
            return false;
        }
        
    }
}
