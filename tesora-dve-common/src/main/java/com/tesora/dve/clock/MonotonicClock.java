// OS_STATUS: public
package com.tesora.dve.clock;

import java.util.Date;


public interface MonotonicClock<T extends Comparable<T>> {
    T nextTimestamp();
    T ensureLaterThan(T happenedBefore);
    Date toDate(T timestamp);
}
