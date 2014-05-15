// OS_STATUS: public
package com.tesora.dve.clock;

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
