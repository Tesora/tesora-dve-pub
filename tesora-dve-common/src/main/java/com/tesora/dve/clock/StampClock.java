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

import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;


public class StampClock implements MonotonicClock<GlobalTimestamp> {
    static final MonotonicLongClock defaultClock = new WalltimeNanos();

    GlobalTimestamp startedAt;

    MonotonicLongClock longClock;

    public StampClock() {
        this(defaultClock);
    }

    public StampClock(MonotonicLongClock longClock) {
        this.longClock = longClock;
        this.startedAt = nextTimestamp();
    }

    public GlobalTimestamp startedAt(){
        return startedAt;
    }

    @Override
    public GlobalTimestamp nextTimestamp() {
        return nextTimestamp( longClock.nextLongTimestamp() );
    }


    GlobalTimestamp nextTimestamp(long currentTime) {
        long someRandom = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        return new GlobalTimestamp(currentTime,someRandom);
    }


    @Override
    public GlobalTimestamp ensureLaterThan(GlobalTimestamp happenedBefore) {
        long nestedLong = happenedBefore.getWallclockInNanos();
        return nextTimestamp(nestedLong);
    }

    @Override
    public Date toDate(GlobalTimestamp timestamp) {
        return longClock.toDateFromLong( timestamp.getWallclockInNanos() );
    }

}
