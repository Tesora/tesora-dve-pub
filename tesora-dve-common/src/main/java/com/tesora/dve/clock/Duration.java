// OS_STATUS: public
package com.tesora.dve.clock;

import java.util.EnumMap;
import java.util.concurrent.TimeUnit;


public class Duration {
    public static final Duration UNDEFINED = new Duration(TimeUnit.SECONDS,-1L);

    static final EnumMap<TimeUnit,String> friendlyStrings;
    static {
        friendlyStrings = new EnumMap<>(TimeUnit.class);
        friendlyStrings.put(TimeUnit.DAYS,"d");
        friendlyStrings.put(TimeUnit.HOURS,"h");
        friendlyStrings.put(TimeUnit.MINUTES,"m");
        friendlyStrings.put(TimeUnit.SECONDS,"s");
        friendlyStrings.put(TimeUnit.MILLISECONDS,"ms");
        friendlyStrings.put(TimeUnit.MICROSECONDS,"μs");
        friendlyStrings.put(TimeUnit.NANOSECONDS,"ns");
    }

    long amount;
    TimeUnit unit;

    public Duration(TimeUnit unit, long amount) {
        this.unit = unit;
        if (amount >= 0) {
            this.amount = amount;
        } else {
            this.amount = -amount;
        }
    }

    public long amount(){
        return amount;
    }

    public TimeUnit unit(){
        return unit;
    }

    public Duration convertTo(TimeUnit unit){
        if (unit == this.unit)
            return this;

        return new Duration(unit, unit.convert(this.amount,this.unit));
    }

    @Override
    public String toString() {
        if (amount < 0)
            return "UNDEFINED";
        else
            return String.format("%d%s",amount,friendlyStrings.get(unit));
    }
}
