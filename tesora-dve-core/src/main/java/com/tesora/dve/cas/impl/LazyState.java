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

import com.tesora.dve.cas.AtomicState;
import com.tesora.dve.cas.StateFactory;


public class LazyState<S> implements AtomicState<S> {
    StateFactory<S> stateFactory;
    AtomicState<S> delegate;

    public LazyState(StateFactory<S> factory, AtomicState<S> delegate) {
        this.stateFactory = factory;
        this.delegate = delegate;
    }

    @Override
    public S get() {
        for (;;){
            S entry = delegate.get();
            if (entry != null)
                return entry;

            S newVal = stateFactory.newInstance();
            if (delegate.compareAndSet(null, newVal))
                return newVal;
        }
    }

    @Override
    public boolean compareAndSet(S expected, S value) {
        return delegate.compareAndSet(expected,value);
    }
}
