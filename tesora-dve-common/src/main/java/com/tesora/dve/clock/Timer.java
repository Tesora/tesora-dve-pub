// OS_STATUS: public
package com.tesora.dve.clock;

public interface Timer {
    boolean isRoot();
    boolean isTopLevel();
    boolean isNested();

    Enum location();

    Timer newSubTimer(Enum location);

    GlobalTimestamp startedAt();
    GlobalTimestamp end();
    GlobalTimestamp end(String... context);
}
