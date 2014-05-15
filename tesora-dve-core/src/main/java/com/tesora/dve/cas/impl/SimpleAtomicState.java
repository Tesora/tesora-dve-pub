// OS_STATUS: public
package com.tesora.dve.cas.impl;

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
