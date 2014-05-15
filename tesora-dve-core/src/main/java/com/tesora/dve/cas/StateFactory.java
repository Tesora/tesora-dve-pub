// OS_STATUS: public
package com.tesora.dve.cas;

public interface StateFactory<S> {
    S newInstance();
}
