// OS_STATUS: public
package com.tesora.dve.clock;

import java.util.Date;


public interface MonotonicLongClock {
    long nextLongTimestamp();
    long ensureLaterThanLong(long happenedBefore);
    Date toDateFromLong(long timestamp);
}
