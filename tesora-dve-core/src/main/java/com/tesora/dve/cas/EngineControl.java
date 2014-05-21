// OS_STATUS: public
package com.tesora.dve.cas;

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


public interface EngineControl<S> {
    void redispatch();
    void clearRedispatch();
    <P> P getProxy(Class<P> firstRequestedInterface, Class... additionalRequestedInterfaces);

    boolean trySetState(S expected, S newState);

    //the expected state is inferred from the starting state at time of engine invocation (needed to support copy-on-invoke).
    boolean trySetState(S newState);

}
