// OS_STATUS: public
package com.tesora.dve.cas;


public interface EngineControl<S> {
    void redispatch();
    void clearRedispatch();
    <P> P getProxy(Class<P> firstRequestedInterface, Class... additionalRequestedInterfaces);

    boolean trySetState(S expected, S newState);

    //the expected state is inferred from the starting state at time of engine invocation (needed to support copy-on-invoke).
    boolean trySetState(S newState);

}
