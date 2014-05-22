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
