// OS_STATUS: public
package com.tesora.dve.locking.impl;

import com.tesora.dve.debug.DebugHandle;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.locking.LocalLockState;
import com.tesora.dve.locking.LockMode;
import com.tesora.dve.lockmanager.LockClient;
import com.tesora.dve.resultset.ResultRow;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class LocalLockStateImpl implements LocalLockState {
    long sharedCount;
    long exclusiveCount;
    LockClient owner;

    ArrayList<ParkSeat> blockers;
    int blockedExclusive = 0;
    int blockedShared = 0;
    boolean immutable;

    EnumSet<LockMode> allowedGrants;


    public LocalLockStateImpl() {
        this.sharedCount = 0L;
        this.exclusiveCount = 0L;
        this.owner = null;

        this.blockers = new ArrayList<ParkSeat>();
        this.allowedGrants = EnumSet.noneOf(LockMode.class);
        immutable = false;
    }

    public LocalLockStateImpl(LocalLockStateImpl other) {
        this.sharedCount = other.sharedCount;
        this.exclusiveCount = other.exclusiveCount;
        this.owner = other.owner;

        this.blockedShared = other.blockedShared;
        this.blockedExclusive =other.blockedExclusive;
        this.allowedGrants = other.allowedGrants;
        this.immutable = false;
        this.blockers = new ArrayList<ParkSeat>();
        for (ParkSeat park : other.blockers){
            ParkSeat copy = new ParkSeat(park);
            this.blockers.add(copy);
        }
    }

    @Override
    public LocalLockStateImpl mutableCopy() {
        return new LocalLockStateImpl(this);
    }

    @Override
    public void setAllowedGrants(EnumSet<LockMode> grants){
        this.allowedGrants = EnumSet.copyOf(grants);
    }

    public List<Latch> nextUnblockSet(){
        if (isBlockersEmpty())
            return Collections.emptyList();


        if ( isNextBlockerExclusive() ){
            return nextExclusiveUnblockSet();
        } else
            return nextSharedUnblockSet();
    }

    public List<Latch> nextExclusiveUnblockSet(){
        if (isBlockersEmpty())
            return Collections.emptyList();

        ArrayList<Latch> retList = new ArrayList<Latch>();

        for (ParkSeat seat : blockers){
            if ( seat.isBlocked() && seat.blockMode() == LockMode.EXCLUSIVE){
                retList.add(seat);
                break;
            }
        }
        return retList;
    }




    public List<Latch> nextSharedUnblockSet(){
        ArrayList<Latch> firstShares = new ArrayList<Latch>();
        ListIterator<ParkSeat> listIter = blockers.listIterator();
        while (listIter.hasNext()){
            ParkSeat caller = listIter.next();
            if (!caller.isBlocked())
                continue;
            if (caller.blockMode() != LockMode.EXCLUSIVE){
                firstShares.add(caller);
            } else {
                //workaround for stall issue.
                if (firstShares.size() > 0)
                    break;
                else
                    continue;
            }
        }
        blockedShared -= firstShares.size();
        return firstShares;
    }

    @Override
    public void unlatch(List<Latch> tickets){
        for (Latch ticket : tickets)
            ticket.unlatch();
    }

    @Override
    public void addShowRow(String name, LockMode globalDeclare, List<ResultRow> rows) {
        //TODO: Not happy about the column meta being in ClusterLockManager and the rows being here, need to get them into the same place.
        /*
        cs.addColumn("lock_name", 255, "varchar", java.sql.Types.VARCHAR);
        cs.addColumn("connection", 255, "varchar", java.sql.Types.VARCHAR);
        cs.addColumn("lock_type", 12, "varchar", java.sql.Types.VARCHAR);
        cs.addColumn("state", 12, "varchar", java.sql.Types.VARCHAR);
        cs.addColumn("reason", 255, "varchar", java.sql.Types.VARCHAR);
        cs.addColumn("originator", 255, "varchar", java.sql.Types.VARCHAR);
        */

        for (ParkSeat seat : blockers){
            ResultRow row = new ResultRow();
            row.addResultColumn(name); //lock_name
            if (seat.client == null)
                row.addResultColumn();
            else
                row.addResultColumn(seat.client.getName());
            row.addResultColumn(seat.isBlocked() ? "blocked" : "acquired" );
            row.addResultColumn("" + globalDeclare);
            row.addResultColumn("" + allowedGrants);
            if (seat.sharedCount < 0)
                throw new RuntimeException("share count less than zero?");
            if (seat.exclusiveCount < 0)
                throw new RuntimeException("exclusive count less than zero?");
            row.addResultColumn(seat.sharedCount);
            row.addResultColumn(seat.exclusiveCount);
            row.addResultColumn(seat.reason);
            rows.add(row);
        }
    }

    public ParkSeat queueBlocker(LockClient client,LockMode mode, String reason){
        int index = findSeat(client,Thread.currentThread());
        ParkSeat blockEntry;
        if (index < 0){
            blockEntry = new ParkSeat(client,Thread.currentThread(),reason, mode);
            blockers.add(blockEntry);
        } else {
            ParkSeat existing = blockers.get(index);
            if ((existing.sharedCount > 0) && (existing.exclusiveCount == 0) && (mode == LockMode.EXCLUSIVE))
                throw new IllegalMonitorStateException("Cannot upgrade a shared lock to an exclusive lock");
            blockEntry = new ParkSeat(client,Thread.currentThread(),reason, mode, existing.sharedCount,existing.exclusiveCount );
            blockers.set(index,blockEntry);
        }
        if (mode == LockMode.EXCLUSIVE)
            blockedExclusive ++;
        else if (mode == LockMode.SHARED)
            blockedShared ++;
        return blockEntry;
    }

    @Override
    public boolean isBlockersEmpty() {
        return blockedExclusive == 0 && blockedShared == 0;
    }

    @Override
    public boolean isNextBlockerExclusive() {
        if (blockedExclusive == 0)
            return false;
        else {
            for (ParkSeat seat : blockers){
                if (seat.isBlocked())
                    return seat.blockMode() == LockMode.EXCLUSIVE;
            }
        }
        return false;
    }

    @Override
    public boolean isNextBlockerShared() {
        if (blockedShared == 0)
            return false;
        else {
            for (ParkSeat seat : blockers){
                if (seat.isBlocked())
                    return seat.blockMode() == LockMode.SHARED;
            }
        }
        return false;
    }

    @Override
    public Latch acquire(LockClient client, final LockMode mode, Thread caller, String reason){
        if (!allowedGrants.contains(mode) || !canAcquire(client,mode,caller))
            return queueBlocker(client,mode, reason);

        if (mode == LockMode.SHARED)
            incShare(client,caller, reason);
        else if (mode == LockMode.EXCLUSIVE)
            incExclusive(client,caller, reason);
        else
            throw new PECodingException("Unexpected lock mode on inc, "+mode);

        return new Latch() {
            public LockMode blockMode() {return mode;}
            public boolean isAcquired() {return true;}
            public void awaitUnlatch() {}
            public void unlatch() {}
        };
    }

    @Override
    public void release(LockClient client,LockMode mode, Thread caller, String reason){
        if (mode == LockMode.SHARED)
            decShare(client,caller,reason);
        else if (mode == LockMode.EXCLUSIVE)
            decExclusive(client,caller,reason);
        else
            throw new PECodingException("Unexpected lock mode on inc, "+mode);
    }

    public void incShare(LockClient client,Thread caller, String reason) {
        if (!canAcquireShared(client,caller))
            throw new PECodingException("Cannot acquire shared lock, not current exclusive owner.  Caller should have been blocked");
        sharedCount++;
        updateUsage(client,caller, 1, 0, reason);
    }


    public void decShare(LockClient client,Thread caller, String reason) {
        if (isExclusive() && !isOwner(client))
            throw new IllegalMonitorStateException("Cannot release shared lock, not current exclusive owner");

        if ( sharedCount <=0 )
            throw new IllegalMonitorStateException("Cannot release shared lock, no matching acquire");

        sharedCount --;
        updateUsage(client,caller, -1, 0, reason);
    }

    public void incExclusive(LockClient client,Thread caller, String reason) {
        if (!canAcquireExclusive(client,caller)){
            throw new PECodingException("Cannot acquire exclusive lock, not current exclusive owner.  Caller should have been blocked");
        }
        this.exclusiveCount ++;
        this.owner = client;
        updateUsage(client,caller, 0, 1, reason);
    }

    public void decExclusive(LockClient client,Thread caller, String reason) {
        if (isExclusive() && !isOwner(client))
            throw new IllegalMonitorStateException("Cannot release exclusive lock, not current exclusive owner");

        if ( exclusiveCount <=0 )
            throw new IllegalMonitorStateException("Cannot release exclusive lock, no matching acquire");

        exclusiveCount--;
        if (exclusiveCount == 0)
            owner = null;

        updateUsage(client,caller, 0, -1, reason);
    }

    private void updateUsage(LockClient client, Thread caller, int deltaShared, int deltaExclusive, String reason) {
        int index = findSeat(client,caller);

        if (index < 0) {
            blockers.add(new ParkSeat(client,caller, reason, deltaShared, deltaExclusive));
        } else {
            ParkSeat existing = blockers.get(index);
            ParkSeat newSeat = new ParkSeat(client,caller, reason, existing.sharedCount + deltaShared, existing.exclusiveCount + deltaExclusive);
            if (newSeat.isUnused())
                blockers.remove(index);
            else
                blockers.set(index, newSeat);
        }
    }

    private int findSeat(LockClient client,Thread caller) {
        int index = -1;
        for (int i=0;i< blockers.size();i++){
            ParkSeat seat = blockers.get(i);
            if (seat.sameClient(client)){
                index = i;
                break;
            }
        }
        return index;
    }

    public boolean canAcquire(LockClient client, LockMode mode, Thread caller){
        if (mode == LockMode.SHARED)
            return canAcquireShared(client,caller);
        else if (mode == LockMode.EXCLUSIVE)
            return canAcquireExclusive(client,caller);
        else
            throw new PECodingException("Unexpected lock mode on canAcquire, "+mode);
    }

    public boolean canAcquireExclusive(LockClient client,Thread caller){
        if (!allowedGrants.contains(LockMode.EXCLUSIVE))
            return false;

        if (!isUnlocked() && (isShared() || !isOwner(client)))
            return false;//we already have a local share lock, or an exclusive and we aren't the owner

        for (ParkSeat seat : blockers){
            if (seat.sameClient(client))
                return true;
            if ( seat.isBlocked())
                return false;//we already have someone else for a lock, get in queue.
        }

        return true;
    }

    public boolean canAcquireShared(LockClient client,Thread caller){
        if (!allowedGrants.contains(LockMode.SHARED))
            return false;

        if ( isExclusive() )
            return false;//we already have a local exclusive lock

        for (ParkSeat seat : blockers){
            if (seat.sameClient(client))
                return true;
            if ( seat.isBlocked() && seat.blockMode() == LockMode.EXCLUSIVE)
                return false;//we already have someone else for a lock, get in queue.
        }
        //no entries.
        return true;
    }

    @Override
    public boolean isUnlocked(){
        return this.exclusiveCount == 0 && this.sharedCount == 0;
    }

    @Override
    public boolean isUnused(){
        return this.blockers.isEmpty();//no blocked threads, no active threads.
    }

    @Override
    public boolean isExclusive() {
        return this.exclusiveCount != 0;
    }

    @Override
    public boolean isShared(){
        return this.exclusiveCount == 0 && this.sharedCount > 0;
    }

    @Override
    public boolean isOwner(LockClient client){
        return this.owner == client;
    }



    public String toString(){
        return String.format("(shared=%s,exclusive=%s,owner=%s)",sharedCount,exclusiveCount,owner);
    }

    @Override
    public void writeTo(DebugHandle displayOut) {
        displayOut.entry("sharedCount", sharedCount);
        displayOut.entry("exclusiveCount", exclusiveCount);
        displayOut.entry("owner", owner);
        displayOut.entry("allowedGrants", allowedGrants);
        displayOut.entry("users","[");
        DebugHandle users = displayOut.nesting();
        for (ParkSeat usage : blockers){
            users.line("" + usage);
        }
        displayOut.line("]");
    }

    public static class ParkSeat implements Latch {
        LockClient client;
        Thread caller;
        String reason;
        int sharedCount;
        int exclusiveCount;

        CountDownLatch latch = new CountDownLatch(1);
        LockMode blockedMode;
        AtomicBoolean unlatched = new AtomicBoolean(false);
        AtomicBoolean finished = new AtomicBoolean(false);


        //only called by queueBlocker
        public ParkSeat(LockClient client,Thread caller, String reason,LockMode mode) {
            this.client = client;
            this.caller = caller;
            this.reason = reason;
            this.blockedMode = mode;
            this.sharedCount = 0;
            this.exclusiveCount = 0;
            unlatched.set(false);
            finished.set(false);
        }

        //only called by updateUsage after an acquire or release
        public ParkSeat(LockClient client,Thread caller, String reason, int sharedCount, int exclusiveCount) {
            this.client = client;
            this.caller = caller;
            this.reason = reason;
            this.blockedMode = null;
            this.sharedCount = sharedCount;
            this.exclusiveCount = exclusiveCount;
            this.finished.set(true);
            this.unlatch();
        }

        //only called by queueBlocker
        public ParkSeat(LockClient client,Thread caller, String reason, LockMode mode,int sharedCount, int exclusiveCount) {
            this.client = client;
            this.caller = caller;
            this.reason = reason;
            this.blockedMode = mode;
            this.sharedCount = sharedCount;
            this.exclusiveCount = exclusiveCount;
            unlatched.set(false);
            this.finished.set(false);
        }

        //copy constructor, called by deep copy of local lock state
        public ParkSeat(ParkSeat other){
            this.client = other.client;
            this.caller = other.caller;
            this.reason = other.reason;
            this.blockedMode = other.blockedMode;
            this.sharedCount = other.sharedCount;
            this.exclusiveCount = other.exclusiveCount;

            //THESE ARE THE SAME UNDERLYING ATOMICS / LATCHES!
            this.unlatched = other.unlatched;
            this.finished = other.finished;
            this.latch = other.latch;
        }

        public boolean sameClient(LockClient otherClient){
            if (this.client == null)
                return otherClient == null;
            else
                return this.client.equals(otherClient);
        }

        public LockMode blockMode(){
            return blockedMode;
        }

        public boolean isAcquired(){
            return this.blockedMode == null;
        }

        public boolean isBlocked(){
            return this.blockedMode != null;
        }

        public String getReason(){
            return reason;
        }

        public boolean isUnused(){
            return isAcquired() && sharedCount == 0 && exclusiveCount == 0;
        }

        public void awaitUnlatch() {
            //            System.out.printf("*** parking %s, mode = %s\n", Thread.currentThread(), blockedMode);
            if (Thread.currentThread() != caller)
                throw new IllegalStateException("Only creator of latch can block on it");
            for(;;){
                try {
                    latch.await();
                    break;
                } catch (InterruptedException e) {
                    //ignore, not interruptable.
                } finally {
                    finished.set(true);
                }
            }
            //            System.out.printf("*** unparked %s, mode = %s\n", Thread.currentThread(), blockedMode);
        }

        public void unlatch(){
            latch.countDown();
            unlatched.set(true);
        }

        public String toString(){
            return String.format("[client=%s,reason=%s,blockmode=%s,blocked=%s,acquired=%s,thread=%s,shared=%s,exclusive=%s]",this.client,this.reason,this.blockedMode,isBlocked(),isAcquired(),caller,sharedCount,exclusiveCount);
        }
    }

}
