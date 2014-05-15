// OS_STATUS: public
package com.tesora.dve.locking;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;


public enum LockMode {
    UNLOCKED,
    ACQUIRING_SHARED,
    ACQUIRING_EXCLUSIVE,
    ACQUIRING_EXCLUSIVE_WITH_SHARE,
    SHARED,
    EXCLUSIVE;

    static final Map<LockMode, EnumSet<LockMode>> grants;
    static final Map<LockMode, EnumSet<LockMode>> conflictsWith;
    static final Map<LockMode, LockMode> wantedMode;
    static final LockMode[] ordinalLookup;

    static {
        //-------------------------------------------------------------------------------------
        EnumMap<LockMode, EnumSet<LockMode>> grantsMap = new EnumMap<LockMode, EnumSet<LockMode>>(LockMode.class);
        grantsMap.put(UNLOCKED, EnumSet.of(UNLOCKED));
        grantsMap.put(ACQUIRING_SHARED,EnumSet.of(UNLOCKED));
        grantsMap.put(ACQUIRING_EXCLUSIVE,EnumSet.of(UNLOCKED));
        grantsMap.put(ACQUIRING_EXCLUSIVE_WITH_SHARE,EnumSet.of(SHARED));
        grantsMap.put(SHARED,EnumSet.of(SHARED));
        grantsMap.put(EXCLUSIVE,EnumSet.of(SHARED,EXCLUSIVE));
        grants = Collections.unmodifiableMap(grantsMap);
        //-------------------------------------------------------------------------------------
        EnumSet<LockMode> none = EnumSet.noneOf(LockMode.class);
        EnumSet<LockMode> notUnlocked = EnumSet.complementOf(EnumSet.of(UNLOCKED));
        EnumSet<LockMode> wantsExclusive = EnumSet.of(ACQUIRING_EXCLUSIVE, ACQUIRING_EXCLUSIVE_WITH_SHARE, EXCLUSIVE);

        EnumMap<LockMode, EnumSet<LockMode>> conflictsMap = new EnumMap<LockMode, EnumSet<LockMode>>(LockMode.class);
        conflictsMap.put(UNLOCKED, none);
        conflictsMap.put(ACQUIRING_SHARED, wantsExclusive);
        conflictsMap.put(ACQUIRING_EXCLUSIVE, notUnlocked);
        conflictsMap.put(ACQUIRING_EXCLUSIVE_WITH_SHARE,notUnlocked);
        conflictsMap.put(SHARED, wantsExclusive);
        conflictsMap.put(EXCLUSIVE,notUnlocked);
        conflictsWith = Collections.unmodifiableMap(conflictsMap);
        //-------------------------------------------------------------------------------------
        Map<LockMode, LockMode> wantedMap = new EnumMap<LockMode, LockMode>(LockMode.class);
        wantedMap.put(UNLOCKED, UNLOCKED);
        wantedMap.put(ACQUIRING_SHARED, SHARED);
        wantedMap.put(ACQUIRING_EXCLUSIVE, EXCLUSIVE);
        wantedMap.put(ACQUIRING_EXCLUSIVE_WITH_SHARE, EXCLUSIVE);
        wantedMap.put(SHARED, SHARED);
        wantedMap.put(EXCLUSIVE, EXCLUSIVE);
        wantedMode = Collections.unmodifiableMap(wantedMap);
        //-------------------------------------------------------------------------------------

        ordinalLookup = LockMode.values();
    }

    public static LockMode forOrdinal(int ord){
        return ordinalLookup[ord];
    }


    public EnumSet<LockMode> grants(){
        return grants.get(this);
    }

    public EnumSet<LockMode> wouldGrant(){
        return grants.get(this.wantedMode());
    }

    public boolean wouldGrant(LockMode mode){
        return grants.get(this).contains(mode);
    }

    public boolean conflictsWith(LockMode mode){
        return conflictsWith.get(this).contains(mode);
    }

    public LockMode wantedMode(){
        return wantedMode.get(this);
    }

    public boolean isAcquiring(){
        return wantedMode.get(this) != this;
    }

}
