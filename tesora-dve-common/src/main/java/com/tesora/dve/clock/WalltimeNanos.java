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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;



public class WalltimeNanos implements MonotonicLongClock, MonotonicClock<Long> {
    public static final long HALF_A_MILLISECOND_IN_NANOS = TimeUnit.MICROSECONDS.toNanos(500);
    public static final int SPINS_ALLOWED_PER_CANDIDATE = 1000000;

    //all clocks created with basic constructor use the same conversion, so drift between clocks in one JVM will be negligible.
    public static final long JVM_NANOS_SINCE_EPOCH = wallclockAtNanoZero();


 /* NOTE: if two instances A and B are created in a single JVM,  A.next() might be ~500K micros AFTER B.next(),
    even if it was created first, because System.currentTimeMillis rounds/clips the nano counter.  Last time I measured,
    the algorithm I'm using had this drift down to less than 100 nanos.  In one JVM, if callers need to avoid this behavior
    and get monotonic ordering, they should share one clock instance, or use multiple clocks and synchronize them using
    ensureLaterThan(), just like you would with multiple remote clocks. -sgossard
 */

    long wallclockAtNanoZero;
    AtomicLong lastNanosViewed;

    public WalltimeNanos(){
        this.wallclockAtNanoZero = JVM_NANOS_SINCE_EPOCH;
        this.lastNanosViewed = new AtomicLong(Long.MIN_VALUE);
        nextLongTimestamp();
    }


    public static long wallclockAtNanoZero() {
        /*
        NOTE: this call is *expensive*, so reuse the value.  We spin the CPU looking for clock times where System.currentTimeMillis() shifts to the next number.

          As far as I can tell, system.currentTimeMillis() and System.nanoTime() are derived from the same underlying hi-res timer.
          With that in mind, two fast calls to nanoTime() or currentTimeMillis() are likely to be derived from very close high-res times,
          but then a lot of precision is lost when ctMillis gets rebucketed by the divide by 1B.  Also, the garbage collector or
          scheduler can come between calls, and create a lot of inaccuracy.  The most accurate timing is when the nano time
          and the milli time are very close together, and backing hi-res timer for the milli time divides evenly with no round-off.
          At this point,
          <code>
               ms=X        ms=X+1
          [------------][MN----------]  *best mapping
          [-----------N][M-----------]  *best mapping
          [------------][NM----------]  *pretty good mapping
          [------------][--------NM--]  *pretty bad mapping, as nano to milli mapping is off as much as ~1ms, avg 500 micros
          [-M----------][------------] ... [------------][--------N---]  *worst mapping due to GC or scheduling issue.
          </code>

          By placing four calls back to back, (nano,millis,millis,nano),  the millisecond values can be tested to check for
          a transition across the milli bucket boundary.  We also take multiple samples and keeping the sample with the
          nano timers that are closest, we tighten the bracketing window

          <code>
                ms=X        ms=X+1
          [-----------N][MMN---------]  * good mapping, but we can't be sure (see next example).
          [------------][--------NMMN]  * bad mapping, but indistinguishable from previous good mapping, discard both.
          [-NM---------][------------][------------][--------MN--]  *Horrible mapping due to GC or scheduling, discard.
          [----------NM][MN----------]  * edge detected, ideal case second milli timer very close to nanotime.
          [-N------M---][------M---N-]  * edge detected, but GC or scheduling caused issues, width between nanotimes will be higher than other samples.
          </code>

         */

        long bestNanoSample = 0;
        long bestWallclockSample = 0;
        long bestDelta = Long.MAX_VALUE;
        int spins = 0;
        int candidatesToCheck = 100;
        while (candidatesToCheck > 0) {
            long nanoStart = System.nanoTime();
            long leadingEdge = System.currentTimeMillis();
            long trailingEdge = System.currentTimeMillis();
            long nanoEnd = System.nanoTime();

            long edgeSpan = trailingEdge - leadingEdge;

            //skip this entry if it doesn't span the milli bucket, unless we've tested 1M samples without a candidate.
            if (edgeSpan != 1 && spins < SPINS_ALLOWED_PER_CANDIDATE){
                spins ++;
                continue;
            }

            long nanoDelta = (nanoEnd - nanoStart);

//            System.out.println("spins to get candidate = "+spins);
            candidatesToCheck--;
            spins=0;

            //keep the samples where the bracketing nano timers are closest.
            if (nanoDelta < bestDelta){
                bestDelta = nanoDelta;

                //calculate the midpoints of the bracketing nanos, and the midpoint (in nanos) of the back to back milli times that span the gap.
                bestWallclockSample = trailingEdge; //trailing edge should be closer to true
                bestNanoSample = nanoStart + (nanoDelta / 2);  //need to use a + ((b-a)/2), not (a+b)/2, because of a+b might overflow.
            }
        }


        //System.out.println("bestDelta=" + bestDelta);
        //when testing, I saw bestDelta was typically around 85 nanos, (roughly 30ns per system call) with a skew between clocks with different nano zeros reduced to ~10-20ns. -sgossard


        long wallclockInNanosFromEpoch = TimeUnit.MILLISECONDS.toNanos(bestWallclockSample) + HALF_A_MILLISECOND_IN_NANOS;
        return wallclockInNanosFromEpoch - bestNanoSample;
    }

    //direct constructor for unit testing.
    protected WalltimeNanos(long wallclockAtNanoZero, AtomicLong atomic){
        this.wallclockAtNanoZero = wallclockAtNanoZero;
        lastNanosViewed = atomic;
    }

    @Override
    public long ensureLaterThanLong(long happenedBefore){
        long next;
        for(;;){
            long now = System.nanoTime();
            long elapsedNanosSinceEpoch = now + wallclockAtNanoZero;
            long last = lastNanosViewed.get();
            next = Math.max(Math.max(elapsedNanosSinceEpoch, last + 1),happenedBefore + 1);
            if (lastNanosViewed.compareAndSet(last,next)){
                break;
            }
        }
        return next;
    }

    @Override
    public long nextLongTimestamp(){
        return ensureLaterThanLong(Long.MIN_VALUE);
    }

    @Override
    public Date toDateFromLong(long nanoTimestamp){
        return new Date(TimeUnit.NANOSECONDS.toMillis(nanoTimestamp));
    }

    @Override
    public Long nextTimestamp() {
        return nextLongTimestamp();
    }

    @Override
    public Long ensureLaterThan(Long happenedBefore) {
        return ensureLaterThanLong(happenedBefore);
    }

    @Override
    public Date toDate(Long timestamp) {
        return toDateFromLong(timestamp);
    }

}
