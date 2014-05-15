// OS_STATUS: public
package com.tesora.dve.debug;


public interface DebugHandle {
    DebugHandle entry(String name, Debuggable entry);
    DebugHandle entry(String name, Object value);
    DebugHandle line(String text);
    DebugHandle nesting();
}
