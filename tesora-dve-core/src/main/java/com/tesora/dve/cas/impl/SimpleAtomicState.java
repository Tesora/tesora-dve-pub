// OS_STATUS: public
package com.tesora.dve.cas.impl;

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

import java.util.concurrent.atomic.AtomicReference;

import com.tesora.dve.cas.AtomicState;


public class SimpleAtomicState<S> implements AtomicState<S> {
    Class<S> stateClass;
    AtomicReference<S> state;

    public SimpleAtomicState(Class<S> stateClass) {
        this(stateClass,null);
    }

    public SimpleAtomicState(Class<S> stateClass,S initialValue) {
        this.stateClass = stateClass;
        this.state = new AtomicReference<S>(initialValue);
    }

    @Override
    public S get() {
        return state.get();
    }

    @Override
    public boolean compareAndSet(S expected, S value) {
        return state.compareAndSet(expected,value);
    }

}
