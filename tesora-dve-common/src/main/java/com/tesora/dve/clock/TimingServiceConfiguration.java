// OS_STATUS: public
package com.tesora.dve.clock;

import com.tesora.dve.exceptions.PEException;

public interface TimingServiceConfiguration {
    void setTimingEnabled(boolean enabled) throws PEException;
}
