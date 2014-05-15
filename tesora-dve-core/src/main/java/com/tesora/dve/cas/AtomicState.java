// OS_STATUS: public
package com.tesora.dve.cas;


public interface AtomicState<S> {
    S get();
    boolean compareAndSet(S expected, S value);
}
