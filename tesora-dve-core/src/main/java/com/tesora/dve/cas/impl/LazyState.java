// OS_STATUS: public
package com.tesora.dve.cas.impl;

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
