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

import java.io.Serializable;
import java.util.concurrent.TimeUnit;


public class GlobalTimestamp implements Comparable<GlobalTimestamp>, Serializable {
    private static final long serialVersionUID = 1L;
    public static final Duration UNDEFINED = new Duration(TimeUnit.SECONDS, -1L);

    long wallclockInNanos;
    long tiebreaker;

    public GlobalTimestamp(long wallclockInNanos, long tiebreaker) {
        this.wallclockInNanos = wallclockInNanos;
        this.tiebreaker = tiebreaker;
    }

    public long getWallclockInNanos(){
        return wallclockInNanos;
    }

    public long getTiebreaker(){
        return tiebreaker;
    }


    @Override
    public int compareTo(GlobalTimestamp other) {
        int compareNanos = Long.compare(this.wallclockInNanos,other.wallclockInNanos);
        if (compareNanos != 0)
            return compareNanos;

        return Long.compare(this.tiebreaker,other.tiebreaker);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GlobalTimestamp that = (GlobalTimestamp) o;

        if (tiebreaker != that.tiebreaker) return false;
        if (wallclockInNanos != that.wallclockInNanos) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (wallclockInNanos ^ (wallclockInNanos >>> 32));
        result = 31 * result + (int) (tiebreaker ^ (tiebreaker >>> 32));
        return result;
    }

    public String toString(){
        //This string format doesn't sort in the same order as the compareTo (the tiebreaker would need to be zero padded), but is cheap to generate and parse. -sgossard
        return String.format("%d.%d",wallclockInNanos,tiebreaker);
    }

    public Duration delta(GlobalTimestamp other){
        if (other == null)
            return Duration.UNDEFINED;

        long deltaNanos = this.wallclockInNanos - other.wallclockInNanos;
        return new Duration(TimeUnit.NANOSECONDS,deltaNanos);
    }
}
