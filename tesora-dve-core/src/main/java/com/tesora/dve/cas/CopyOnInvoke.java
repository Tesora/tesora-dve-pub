// OS_STATUS: public
package com.tesora.dve.cas;


public interface CopyOnInvoke<S> {
    S mutableCopy(EngineControl<S> control);
}
