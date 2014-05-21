// OS_STATUS: public
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


public class NoopTimingService implements TimingService{
    public static final NoopTimingService SERVICE = new NoopTimingService();
    public static final NoopTimer NOOP_TIMER = new NoopTimer();

    enum TimingDesc { NOOP_ROOT }

    @Override
    public Timer getTimerOnThread() {
        return NOOP_TIMER;
    }

    @Override
    public void detachTimerOnThread() {
    }

    @Override
    public Timer attachTimerOnThread(Timer parent) {
        return NOOP_TIMER;
    }

    @Override
    public Timer startSubTimer(Enum location) {
        return NOOP_TIMER;
    }

    static final class NoopTimer implements Timer {
        static final GlobalTimestamp NOOP_TIME = new GlobalTimestamp(System.currentTimeMillis() * 1000000L,System.currentTimeMillis());

        @Override
        public boolean isRoot() {
            return true;
        }

        @Override
        public boolean isTopLevel() {
            return false;
        }

        @Override
        public boolean isNested() {
            return false;
        }

        @Override
        public Enum location() {
            return TimingDesc.NOOP_ROOT;
        }

        @Override
        public Timer newSubTimer(Enum location) {
            return this;
        }

        @Override
        public GlobalTimestamp startedAt() {
            return NOOP_TIME;
        }

        @Override
        public GlobalTimestamp end() {
            return NOOP_TIME;
        }

        @Override
        public GlobalTimestamp end(String... context) {
            return NOOP_TIME;
        }
    }
}
