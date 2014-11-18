package com.tesora.dve.standalone;

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

import io.netty.util.ResourceLeakDetector;
import io.netty.util.internal.logging.InternalLogLevel;
import io.netty.util.internal.logging.InternalLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class NettyLeakIntercept {
    static final Logger logger = LoggerFactory.getLogger(NettyLeakIntercept.class);

    //intended to wrap the normal logger used by netty's ResourceLeakDetector, to intercept leak warnings that Netty reports via the logger, for testing only!
    public static LeakCounter installLeakTrap(){
        try {
            Field loggerField = ResourceLeakDetector.class.getDeclaredField("logger");
            loggerField.setAccessible(true);

            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(loggerField, loggerField.getModifiers() & ~Modifier.FINAL);

            InternalLogger leakLogger = (InternalLogger)loggerField.get(null);
            if (leakLogger instanceof LeakCounter) {
                logger.debug("leak counter already installed.");
                return (LeakCounter) leakLogger;
            }else if (leakLogger != null){
                logger.debug("monkeypatching leak counter into Netty leak detection.");
                LeakTrap trap = new LeakTrap(leakLogger);
                loggerField.set(null, trap);
                return trap;
            } else {
                throw new NullPointerException("Netty ResourceLeakDetector had null logger reference?");
            }
        } catch (Throwable t){
            t.printStackTrace();
            logger.warn("Couldn't monkeypatch leak counter into netty leak detection, returning noop counter to tests.");
            return new NoopLeakTrap();
        }
    }



    public interface LeakCounter {
        long getEmittedLeakCount();
        long clearEmittedLeakCount();
    }


    public static class NoopLeakTrap implements LeakCounter {
        @Override
        public long getEmittedLeakCount() {
            return 0;
        }

        @Override
        public long clearEmittedLeakCount() {
            return 0;
        }
    }

    public static class LeakTrap implements InternalLogger, LeakCounter {
        InternalLogger delegate;
        AtomicLong leakErrorsEmitted = new AtomicLong(0L);

        public LeakTrap(InternalLogger delegate) {
            this.delegate = delegate;
        }

        public long getEmittedLeakCount(){
            return leakErrorsEmitted.get();
        }

        public long clearEmittedLeakCount(){
            return leakErrorsEmitted.getAndSet(0L);
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public boolean isTraceEnabled() {
            return delegate.isTraceEnabled();
        }

        @Override
        public void trace(String msg) {
            delegate.trace(msg);
        }

        @Override
        public void trace(String format, Object arg) {
            delegate.trace(format, arg);
        }

        @Override
        public void trace(String format, Object argA, Object argB) {
            delegate.trace(format, argA, argB);
        }

        @Override
        public void trace(String format, Object... arguments) {
            delegate.trace(format, arguments);
        }

        @Override
        public void trace(String msg, Throwable t) {
            delegate.trace(msg, t);
        }

        @Override
        public boolean isDebugEnabled() {
            return delegate.isDebugEnabled();
        }

        @Override
        public void debug(String msg) {
            delegate.debug(msg);
        }

        @Override
        public void debug(String format, Object arg) {
            delegate.debug(format, arg);
        }

        @Override
        public void debug(String format, Object argA, Object argB) {
            delegate.debug(format, argA, argB);
        }

        @Override
        public void debug(String format, Object... arguments) {
            delegate.debug(format, arguments);
        }

        @Override
        public void debug(String msg, Throwable t) {
            delegate.debug(msg, t);
        }

        @Override
        public boolean isInfoEnabled() {
            return delegate.isInfoEnabled();
        }

        @Override
        public void info(String msg) {
            delegate.info(msg);
        }

        @Override
        public void info(String format, Object arg) {
            delegate.info(format, arg);
        }

        @Override
        public void info(String format, Object argA, Object argB) {
            delegate.info(format, argA, argB);
        }

        @Override
        public void info(String format, Object... arguments) {
            delegate.info(format, arguments);
        }

        @Override
        public void info(String msg, Throwable t) {
            delegate.info(msg, t);
        }

        @Override
        public boolean isWarnEnabled() {
            return delegate.isWarnEnabled();
        }

        @Override
        public void warn(String msg) {
            delegate.warn(msg);
        }

        @Override
        public void warn(String format, Object arg) {
            delegate.warn(format, arg);
        }

        @Override
        public void warn(String format, Object... arguments) {
            delegate.warn(format, arguments);
        }

        @Override
        public void warn(String format, Object argA, Object argB) {
            delegate.warn(format, argA, argB);
        }

        @Override
        public void warn(String msg, Throwable t) {
            delegate.warn(msg, t);
        }

        @Override
        public boolean isErrorEnabled() {
            return delegate.isErrorEnabled();
        }

        @Override
        public void error(String msg) {
            incrementLeak();
            delegate.error(msg);
        }

        @Override
        public void error(String format, Object arg) {
            incrementLeak();
            delegate.error(format, arg);
        }

        private void incrementLeak() {
            leakErrorsEmitted.incrementAndGet();
        }

        @Override
        public void error(String format, Object argA, Object argB) {
            incrementLeak();
            delegate.error(format, argA, argB);
        }

        @Override
        public void error(String format, Object... arguments) {
            incrementLeak();
            delegate.error(format, arguments);
        }

        @Override
        public void error(String msg, Throwable t) {
            incrementLeak();
            delegate.error(msg, t);
        }

        @Override
        public boolean isEnabled(InternalLogLevel level) {
            return delegate.isEnabled(level);
        }

        @Override
        public void log(InternalLogLevel level, String msg) {
            if (level == InternalLogLevel.ERROR)
                incrementLeak();
            delegate.log(level, msg);
        }

        @Override
        public void log(InternalLogLevel level, String format, Object arg) {
            if (level == InternalLogLevel.ERROR)
                incrementLeak();
            delegate.log(level, format, arg);
        }

        @Override
        public void log(InternalLogLevel level, String format, Object argA, Object argB) {
            if (level == InternalLogLevel.ERROR)
                incrementLeak();
            delegate.log(level, format, argA, argB);
        }

        @Override
        public void log(InternalLogLevel level, String format, Object... arguments) {
            if (level == InternalLogLevel.ERROR)
                incrementLeak();
            delegate.log(level, format, arguments);
        }

        @Override
        public void log(InternalLogLevel level, String msg, Throwable t) {
            if (level == InternalLogLevel.ERROR)
                incrementLeak();
            delegate.log(level, msg, t);
        }
    }
}
