package com.tesora.dve.locking.impl;

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

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.locking.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tesora.dve.cas.CopyOnInvoke;
import com.tesora.dve.cas.EngineControl;
import com.tesora.dve.debug.DebugHandle;
import com.tesora.dve.debug.Debuggable;
import com.tesora.dve.membership.MembershipView;
import com.tesora.dve.locking.LocalLockState.Latch;
import com.tesora.dve.lockmanager.LockClient;
import com.tesora.dve.resultset.ResultRow;

public class ClusterLockImpl implements ClusterLock, Debuggable, CopyOnInvoke<ClusterLockImpl> {
    static final Logger log = LoggerFactory.getLogger(ClusterLockImpl.class);

    String name;
    long sequence;
    EngineControl<ClusterLockImpl> engine;
    LocalLockState local;
    GlobalLockState global;

    public ClusterLockImpl(String name, EngineControl<ClusterLockImpl> engine, GlobalLockState global) {
        this.name = name;
        this.sequence = 0L;
        this.engine = engine;
        this.local = new LocalLockStateImpl();
        this.global = global;
    }

    protected ClusterLockImpl(ClusterLockImpl other, EngineControl<ClusterLockImpl> engine) {
        this.name = other.name;
        this.sequence = other.sequence + 1;
        this.engine = engine;
        this.local = other.local.mutableCopy();
        this.global = other.global.mutableCopy();
    }

    @Override
    public ClusterLockImpl mutableCopy(EngineControl<ClusterLockImpl> control) {
        return new ClusterLockImpl(this,control);
    }

    public boolean isUnused(){
        return local.isUnused();
    }


    @Override
    public void sharedLock(LockClient client, String reason) {
        acquireLock(client,LockMode.SHARED, reason);
    }

    @Override
    public void exclusiveLock(LockClient client, String reason) {
        acquireLock(client,LockMode.EXCLUSIVE,reason);
    }

    @Override
    public void sharedUnlock(LockClient client, String reason) {
        releaseLock(client,LockMode.SHARED,reason);
    }

    @Override
    public void exclusiveUnlock(LockClient client, String reason) {
        releaseLock(client,LockMode.EXCLUSIVE,reason);
    }

    private void acquireLock(LockClient client,LockMode mode , String reason) {
        local.setAllowedGrants(global.getGrantedModes());
        Latch acquireResult = local.acquire(client,mode,Thread.currentThread(), reason);
        Runnable ticket = chooseLazyGlobalState(global, local, RunnableNoop.NOOP);
        boolean changedState = engine.trySetState(this);
        if (changedState) {
            ticket.run();
            acquireResult.awaitUnlatch();
            if (acquireResult.isAcquired()){
                engine.clearRedispatch();
                return;
            }
        }
        engine.redispatch();
    }

    private void releaseLock(LockClient client, LockMode mode, String reason) {
        local.setAllowedGrants(global.getGrantedModes());
        local.release(client, mode, Thread.currentThread(), reason);

        Runnable ticket = chooseLazyGlobalState(global, local, RunnableNoop.NOOP);
        List<Latch> unblock = generateUnblockList(global, local);
        if (engine.trySetState(this)){
            ticket.run();
            local.unlatch(unblock);
        } else {
            engine.redispatch();
        }
    }

    @Override
    public void onMessage(IntentEntry message) {
        if (global.isStale(message)){
            log.debug("GlobalLock[{},{}], ignoring stale message {}", new Object[]{name, sequence, message});
            return;
        }
        Runnable ticket = chooseLazyGlobalState(global, local,  global.process(message) );
        List<Latch> unblock = generateUnblockList(global,local);
        local.setAllowedGrants(global.getGrantedModes());

        if (engine.trySetState(this)){
            log.debug("GlobalLock[{},{}], processed message {}, new state={}, sending={}, unblocking={}", new Object[]{name,sequence,message, this,ticket,unblock});
            ticket.run();
            local.unlatch(unblock);
        } else {
            engine.redispatch();
        }
    }

    public void addShowRow(List<ResultRow> rows){
        local.setAllowedGrants(global.getGrantedModes());
        local.addShowRow(name,global.getDeclaredMode(),rows);
    }


    @Override
    public void writeTo(DebugHandle displayOut) {
        displayOut.entry("name",name);
        displayOut.entry("sequence", sequence - 1); //subtract one, we are on a uncommitted copy.
        displayOut.entry("local", local);
        displayOut.entry("global", global);
    }

    @Override
    public void onMembershipChange(MembershipView membershipView){
        Runnable ticket = global.membershipChange(membershipView);
        //local.setAllowedGrants(global.getGrantedModes());
        if ( engine.trySetState(this) ){
            ticket.run();
        } else {
            engine.redispatch();
        }
    }

    public String toString(){
        return String.format("GlobalLock[name=%s,seq=%s,local=%s,global=%s]",name,sequence,local,global);
    }

    protected static Runnable chooseLazyGlobalState(GlobalLockState global, LocalLockState localBlockers, Runnable defaultChoice) {
        if (global.getDeclaredMode().isAcquiring() || !localBlockers.isUnlocked())
            return defaultChoice;  //we already have local locks, or are acquiring, return default choice, which is NOOP or user requested state.

        boolean grantedShared = global.isGranted(LockMode.SHARED);
        boolean grantedExclusive = global.isGranted(LockMode.EXCLUSIVE);
        boolean blockingShared = global.isBlocking(LockMode.SHARED);
        boolean blockingExclusive = global.isBlocking(LockMode.EXCLUSIVE);
        boolean noGlobalBlockers = global.getBlockedModes().isEmpty();
        boolean needShared = localBlockers.isNextBlockerShared();
        boolean needExclusive = localBlockers.isNextBlockerExclusive();
        boolean noLocalWaiters = localBlockers.isBlockersEmpty();

        if (global.getDeclaredMode() == LockMode.UNLOCKED){
            if (needShared)
                return global.acquiringShared();
            else if (needExclusive)
                return global.acquiringExclusive();
        }

        //no one is waiting, so drop to the lowest requested by others.
        if (noLocalWaiters){
            if (blockingExclusive)
                return global.unlocking();
            if (blockingShared && grantedExclusive)
                return global.downgradingToShared();
            if (noGlobalBlockers)
                return defaultChoice;
        }

        //ok, so we are locally unlocked, but have local local.

        if (grantedExclusive){
            if (needExclusive)
                return defaultChoice;
            if (blockingExclusive)
                return global.unlocking();
            else {
                return global.downgradingToShared();
            }
        }

        if (blockingExclusive)
            return global.unlocking();

        if (needExclusive)
            return global.acquiringExclusive();

        return defaultChoice;

    }

    protected static List<Latch> generateUnblockList(GlobalLockState globalView, LocalLockState localBlockers) {
        if (globalView.isAcquiring())
            return new ArrayList<Latch>();

        List<Latch> unblock = new ArrayList<Latch>();
        if (globalView.isGranted(LockMode.EXCLUSIVE) && localBlockers.isUnlocked()){
            unblock = localBlockers.nextUnblockSet();
        } else if (globalView.isGranted(LockMode.SHARED) && !localBlockers.isExclusive()){
            unblock = localBlockers.nextSharedUnblockSet();
        }
        return unblock;
    }

}
