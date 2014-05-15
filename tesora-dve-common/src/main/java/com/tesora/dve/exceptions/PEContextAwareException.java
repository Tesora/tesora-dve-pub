// OS_STATUS: public
package com.tesora.dve.exceptions;

import com.tesora.dve.common.PEContext;

/**
 * Marker interface for exceptions that capture a PEContext when instantiated.
 */
public interface PEContextAwareException {

	PEContext getContext();

}
