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

import com.tesora.dve.clock.MonotonicLongClock;
import com.tesora.dve.debug.DebugHandle;
import com.tesora.dve.groupmanager.GroupTopic;
import com.tesora.dve.groupmanager.MembershipView;
import com.tesora.dve.locking.GlobalLockState;
import com.tesora.dve.locking.LockMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class GlobalLockStateImpl implements GlobalLockState {
    static final Logger log = LoggerFactory.getLogger(GlobalLockStateImpl.class);
    MonotonicLongClock clock;
    GroupTopic<IntentEntry> topic;
    MembershipView membership;
    ArrayList<IntentEntry> registrations;
    AtomicLong transmitCount;

    public GlobalLockStateImpl(MonotonicLongClock clock, GroupTopic<IntentEntry> topic, MembershipView membership) {
        this.clock = clock;
        this.topic = topic;
        this.membership = membership;
        this.registrations = new ArrayList<IntentEntry>();
        this.registrations.add( new IntentEntry(membership.getMyAddress(), LockMode.UNLOCKED,clock.nextLongTimestamp()) );
        this.transmitCount = new AtomicLong(0L);
    }

    GlobalLockStateImpl(MonotonicLongClock clock, GroupTopic<IntentEntry> topic, MembershipView membership, ArrayList<IntentEntry> registrations, AtomicLong transmitted) {
        this.clock = clock;
        this.topic = topic;
        this.membership = membership;
        this.registrations = registrations;
        this.transmitCount = transmitted;
    }

    public Runnable republish(final String message){
        if (findFor(membership.getMyAddress()) >= 0){ //TODO: if this is >=0, it causes a hang on shutdown, but >0 seems wrong, since it won't rebroadcast if we are first in list.
            return new SimplePublishTicket( freshenMyEntry(true) ){
                public void run() {
                    log.warn(String.format(message));
                    super.run();
                }
            };
        } else
            return RunnableNoop.NOOP;
    }

    public Runnable membershipChange(MembershipView newView){
        this.membership = newView;
        ListIterator<IntentEntry> iter = registrations.listIterator();
        final ArrayList<IntentEntry> dropped = new ArrayList<IntentEntry>();
        while (iter.hasNext()){
            IntentEntry entry = iter.next();
            if ( !entry.isFor(this.membership.getMyAddress()) && (!this.membership.isInQuorum() || !this.membership.activeQuorumMembers().contains(entry.member)) ){
                dropped.add(entry);
                iter.remove();
            }
        }
        if (dropped.size() > 0){
            Collections.sort(registrations);
        }

        return republish(String.format("Quorum membership changed, dropping entries for , %s",dropped));
    }

    @Override
    public GlobalLockState mutableCopy() {
        ArrayList<IntentEntry> copyRegs = new ArrayList<IntentEntry>();
        copyRegs.addAll(registrations);
        return new GlobalLockStateImpl(this.clock,this.topic,this.membership,copyRegs, transmitCount);
    }

    IntentEntry myEntry(){
        int index = findFor(membership.getMyAddress());
        if (index < 0)
            return null;
        else
            return registrations.get(index);
    }

    @Override
    public LockMode getDeclaredMode(){
        int myIndex = findFor(membership.getMyAddress());
        return registrations.get(myIndex).state;
    }

    public boolean isAcquiring(){
        return getDeclaredMode().isAcquiring();
    }

    @Override
    public EnumSet<LockMode> getGrantedModes(){
        verifyInQuorum();
        int myIndex = findFor(membership.getMyAddress());
        if (myIndex < 0)
            return EnumSet.noneOf(LockMode.class);
        return getGrantedModes(myIndex);
    }

    private void verifyInQuorum() throws IllegalMonitorStateException {
        if (! membership.isInQuorum() )
            throw new IllegalMonitorStateException("Current member is not in quorum, cannot process lock request");
    }

    public boolean isGranted(LockMode mode){
        verifyInQuorum();
        return getGrantedModes().contains(mode);
    }

    public EnumSet<LockMode> getGrantedModes(int index){
        IntentEntry entry = registrations.get(index);

        if ( !fullyAcknowledged( index ) )
            return EnumSet.of(LockMode.UNLOCKED);

        EnumSet<LockMode> actualGrant = entry.state.wouldGrant().clone();

        for (int i=0;i < index;i++){
            IntentEntry existing = registrations.get(i);
            if (existing.state.conflictsWith(LockMode.EXCLUSIVE)){
                actualGrant.remove(LockMode.EXCLUSIVE);
            }
            if (existing.state.conflictsWith(LockMode.SHARED)){
                actualGrant.remove(LockMode.SHARED);
            }
            if (actualGrant.size() == 0)
                break;
        }
        if (actualGrant.size() == 0)
            return EnumSet.of(LockMode.UNLOCKED);
        else
            return actualGrant;
    }

    public EnumSet<LockMode> getBlockedModes(){
        int myIndex = findFor(membership.getMyAddress());
        return getBlockedModes(myIndex);
    }

    public boolean isBlocking(LockMode mode){
        return getBlockedModes().contains(mode);
    }

    public EnumSet<LockMode> getBlockedModes(int index){
        IntentEntry entry = registrations.get(index);
        EnumSet<LockMode> blocking = EnumSet.noneOf(LockMode.class);

        for (int i=index+1;i < registrations.size();i++){
            IntentEntry existing = registrations.get(i);
            if (existing.state.conflictsWith(entry.state))
                blocking.add( existing.state.wantedMode() );
            if (blocking.size() == 2) //both, so exit.
                break;
        }
        return blocking;
    }

    public boolean fullyAcknowledged(int index){
        return numberOfMissingAcksForEntry(index) <=0;
    }

    public int numberOfMissingAcksForEntry(int index){
        return membership.activeQuorumMembers().size() - numberThatHaveAckedEntry(index);
    }

    public int numberThatHaveAckedEntry(int index){
        if (index < 0)
            return 0;
        IntentEntry entry = registrations.get(index);
        int count = 0;
        for (int i=0;i<registrations.size();i++){
            if (entry.isAckedBy(registrations.get(i)))
                count ++;
        }
        return count;
    }

    public int numberNotAckedByEntry(int index){
        if (index < 0)
            return registrations.size();
        IntentEntry entry = registrations.get(index);
        int count = 0;
        for (int i=0;i<registrations.size();i++){
            if (!registrations.get(i).isAckedBy(entry))
                count ++;
        }
        return count;
    }

    @Override
    public Runnable acquiringExclusive() {
        verifyInQuorum();
//        System.out.println(membership.getMyAddress() + " acq excl");
        IntentEntry entry = new IntentEntry(membership.getMyAddress(), LockMode.ACQUIRING_EXCLUSIVE,clock.nextLongTimestamp());
        return process(entry,true);
    }

    @Override
    public Runnable  acquiringShared() {
        verifyInQuorum();
        IntentEntry entry = new IntentEntry(membership.getMyAddress(), LockMode.ACQUIRING_SHARED,clock.nextLongTimestamp());
        return process(entry,true);
    }

    @Override
    public Runnable  unlocking() {
        verifyInQuorum();
        IntentEntry entry = new IntentEntry(membership.getMyAddress(), LockMode.UNLOCKED,clock.nextLongTimestamp());
        return process(entry,true);
    }

    @Override
    public Runnable downgradingToShared() {
        verifyInQuorum();
        if (!isGranted(LockMode.EXCLUSIVE))
            throw new IllegalStateException("Cannot downgrade to shared, not granted exclusive");

        IntentEntry entry = new IntentEntry(membership.getMyAddress(), LockMode.SHARED, clock.nextLongTimestamp(),myEntry().lastUnlockedOrAcquired);
        return process(entry,true);
    }

    int findFor(InetSocketAddress someAddr){
        for (int i=0;i < registrations.size();i++){
            IntentEntry entry = registrations.get(i);
            if (entry.isFor(someAddr))
                return i;
        }
        return -1;
    }

    @Override
    public boolean isStale(IntentEntry someEntry) {
        updateClockIfNeeded(someEntry);
//        System.out.println(membership.getMyAddress() + " :: onMessage entry (via is stale) ==> "+entry);
        if (someEntry.isFor(membership.getMyAddress()) && membership.activeQuorumMembers().size() > 1)
            return true;

        //ignore messages that aren't from active quorum members.
        if (!membership.activeQuorumMembers().contains(someEntry.member))
            return true;
//
//        int index = findFor(entry.member);
//        if (index < 0)
//            return false;
//        IntentEntry existing = registrations.get(index);
//        boolean isStale = entry.published <= existing.published;
//        System.out.printf("stale check[recvr=%s,stale=%s] existing=%s , entry=%s\n",membership.getMyAddress(),isStale,existing,entry);
//        return isStale;
        return false;
    }

    @Override
    public Runnable process(IntentEntry someEntry) {
        return process(someEntry,false);
    }


    protected Runnable process(IntentEntry someEntry,boolean locallyTriggered) {
        updateClockIfNeeded(someEntry);
        if (!membership.isInQuorum())
            return RunnableNoop.NOOP;

        boolean causedTransition = false;
        int updatedEntryIndex = findFor(someEntry.member);
        int myIndex = findFor(membership.getMyAddress());
        IntentEntry myEntry;
        if (myIndex >= 0)
            myEntry = registrations.get(myIndex);
        else
            myEntry = null;
        boolean needsAck = false;

        if (updatedEntryIndex >=0 && !registrations.get(updatedEntryIndex).isPublishedBefore(someEntry) ){ //same entry
            return RunnableNoop.NOOP;
        }

        if (updatedEntryIndex < 0){
            registrations.add(someEntry);
            needsAck = true;
        } else {
            IntentEntry existing = registrations.get(updatedEntryIndex);
            needsAck = existing.isEnteredBefore(someEntry);
            registrations.set(updatedEntryIndex,someEntry);
        }

        Collections.sort(registrations);//need to sort to get ordering for isGrantedXXX() stuff to work right.

        myIndex = findFor(membership.getMyAddress());
        IntentEntry myEntryAfterInsert = myEntry();
        long newTS = clock.nextLongTimestamp();

        EnumSet<LockMode> grantedModes = getGrantedModes();
        if (myEntryAfterInsert != null && grantedModes.contains(myEntryAfterInsert.state.wantedMode())){
            IntentEntry element = new IntentEntry(membership.getMyAddress(), myEntryAfterInsert.state.wantedMode(), newTS, myEntryAfterInsert.lastUnlockedOrAcquired);
            registrations.set(myIndex, element);
            causedTransition = true;
        } else if (myEntryAfterInsert != null && myEntryAfterInsert.state == LockMode.ACQUIRING_EXCLUSIVE && grantedModes.contains(LockMode.SHARED)){
            IntentEntry element = new IntentEntry(membership.getMyAddress(), LockMode.ACQUIRING_EXCLUSIVE_WITH_SHARE, newTS, myEntryAfterInsert.lastUnlockedOrAcquired);
            registrations.set(myIndex, element);
            causedTransition = true;
        }

//        int numberWaitingForAfterTransition = numberOfMissingAcksForEntry( findFor(membership.getMyAddress() ));
//        boolean remoteRequested = (!someEntry.isFor(membership.getMyAddress()) && someEntry.wantsAck());
//        boolean remoteOutOfDate = (numberINeedToAckBeforeTransition > 0 || numberWaitingForAfterTransition >0);
        boolean remoteOutOfDate = (!someEntry.isFor(membership.getMyAddress()) && needsAck );
//        boolean triggeredUpdate = locallyTriggered || causedTransition || remoteOutOfDate;
        boolean triggeredUpdate = locallyTriggered ||  remoteOutOfDate;
//        System.out.printf("%s, received %s, needed to ack %s messages, locallyTriggered=%s, causedTransition=%s, sendingUpdate=%s\n",membership.getMyAddress(),someEntry,numberINeedToAckBeforeTransition,locallyTriggered,causedTransition,triggeredUpdate);
        if (triggeredUpdate){
            //freshen up timestamp before we transmit.
            return new SimplePublishTicket( freshenMyEntry(locallyTriggered) );

        } else {
            Collections.sort(registrations);
//            System.out.printf("%s, needed to ack %s messages, and was missing %s acks, not sending\n",membership.getMyAddress(),numberNeedingMyAcksBefore,missingAcksForMe);
            return RunnableNoop.NOOP;
        }
    }

    private IntentEntry freshenMyEntry(boolean locallyTriggered) {
        int myIndex;
        IntentEntry myAfter;
        myIndex = findFor(membership.getMyAddress());
        myAfter = registrations.get(myIndex);
        IntentEntry element = new IntentEntry(membership.getMyAddress(), myAfter.state, clock.nextLongTimestamp() , myAfter.lastUnlockedOrAcquired);
        registrations.set(myIndex, element);
        myAfter = element;
        Collections.sort(registrations);
        return myAfter;
    }

    @Override
    public void writeTo(DebugHandle displayOut){
        int index = findFor(membership.getMyAddress());
        IntentEntry myEntry = registrations.get(index);
        displayOut.entry("membership", membership);
        displayOut.entry("state", myEntry.state);
        displayOut.entry("transmitCount", transmitCount.get());
        displayOut.entry("needAcksFrom", numberOfMissingAcksForEntry(index));
        displayOut.entry("needMeToAck", numberNotAckedByEntry(index));
        displayOut.entry("granted", getGrantedModes(index));
        displayOut.entry("blocking", getBlockedModes(index));
        for (IntentEntry entry : registrations){
            displayOut.line("" + entry + " [" + (myEntry.isAckedBy(entry) ? "ACKS ME]" : "ACKS ME NOT]"));
        }        
    }

    void updateClockIfNeeded(IntentEntry someEntry) {
        clock.ensureLaterThanLong(someEntry.published);
        clock.ensureLaterThanLong(someEntry.lastUnlockedOrAcquired);
    }


    public String toString(){
        int index = findFor(membership.getMyAddress());
        IntentEntry entry = myEntry();
        return String.format("TopicGroupList[membership=%s,entry.state=%s,entries=%s,needAcksFrom=%s, needMeToAck=%s, transmitCount=%s]", membership, entry.state, registrations.size(), numberOfMissingAcksForEntry(index), numberNotAckedByEntry(index), transmitCount.get());
    }



    public class SimplePublishTicket implements Runnable {
        IntentEntry entry;

        public SimplePublishTicket(IntentEntry entry) {
            this.entry = entry;
        }

        @Override
        public void run() {
//            System.out.printf("Member %s , sending ==> %s\n",membership.getMyAddress(),entry);
            transmitCount.incrementAndGet();
            topic.publish(entry);
        }

        public String toString(){
            return String.format("PublishTicket(%s,%s)",membership.getMyAddress(),entry);
        }
    }
}
