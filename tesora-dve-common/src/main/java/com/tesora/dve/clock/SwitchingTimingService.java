package com.tesora.dve.clock;

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

import com.tesora.dve.exceptions.PEException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicReference;


public class SwitchingTimingService implements TimingService, TimingServiceConfiguration {
    static final Logger logger = LoggerFactory.getLogger(SwitchingTimingService.class);

    static TimingService NOOP = new NoopTimingService();

    AtomicReference<TimingService> delegate = new AtomicReference<>(NOOP);
    Path baseLogDir;

    public SwitchingTimingService(Path baseLogDir) {
        if (baseLogDir == null){
            try {
                this.baseLogDir = Files.createTempDirectory("tesora_perf");
                logger.warn("Base logging directory was not set, putting perf trace logs in temp dir, "+this.baseLogDir);
            } catch (IOException e) {
                logger.warn("Base logging directory was not set and a temp directory could not be created, perf trace logging cannot be enabled.");
                this.baseLogDir = null;
            }
        } else {
            this.baseLogDir = baseLogDir;
        }
    }

    @Override
    public void setTimingEnabled(boolean enabled) throws PEException {
        if (enabled)
            enableTiming();
        else
            disableTiming();
    }

    private void disableTiming() {
        changeServiceAtomically(NOOP);
    }

    private void enableTiming() throws PEException {
        if (logger.isDebugEnabled())
            logger.debug("Trying to enable perf trace, base dir is "+baseLogDir);

        if (baseLogDir == null)
            return;  //no target directory, ignore.

        Path perfLogFile = null;
        try {
            StampClock clock = new StampClock();
            perfLogFile = baseLogDir.resolve("perfTrace.log");
            DefaultTimingService nextService = new DefaultTimingService(clock,perfLogFile);
            changeServiceAtomically(nextService);
        } catch (Exception e){
            String message = "Could not create performance log file, " + perfLogFile;
            logger.warn(message);
            throw new PEException(message,e);
        }


    }

    private void changeServiceAtomically(TimingService nextService) {
        for(;;){
            TimingService previous = delegate.get();
            if (delegate.compareAndSet(previous,nextService)){
                //we could notify the new timing service it is installed, but it may already be receiving requests.
                closeServiceIfNeeded(previous);
                break;
            }
        }
    }

    private void closeServiceIfNeeded(TimingService current) {
        if (current instanceof Closeable){
            Closeable closeMe = (Closeable)current;
            try{
                closeMe.close();
            } catch (Exception e){
                logger.warn("Problem closing existing timing service, {} ,exception was ",e);
            }
        }
    }

    @Override
    public Timer getTimerOnThread() {
        return delegate.get().getTimerOnThread();
    }

    @Override
    public void detachTimerOnThread() {
        delegate.get().detachTimerOnThread();
    }

    @Override
    public Timer attachTimerOnThread(Timer parent) {
        return delegate.get().attachTimerOnThread(parent);
    }

    @Override
    public Timer startSubTimer(Enum location) {
        return delegate.get().startSubTimer(location);
    }
}
