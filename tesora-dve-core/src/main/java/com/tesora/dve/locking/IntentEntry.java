package com.tesora.dve.locking;

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

import java.net.InetSocketAddress;


public class IntentEntry implements Comparable<IntentEntry> {
    public InetSocketAddress member;
    public long published;
    public long lastUnlockedOrAcquired;
    public LockMode state;

    public IntentEntry(InetSocketAddress member,LockMode state,long published) {
        this(member,state,published,published);
    }

    public IntentEntry(InetSocketAddress member,LockMode state,long published, long lastUnlockedOrAcquired) {
        this.member = member;
        this.published = published;
        this.lastUnlockedOrAcquired = lastUnlockedOrAcquired;
        this.state = state;
    }

    public boolean isFor(InetSocketAddress myID) {
        return this.member.equals(myID);
    }

    protected int comparePublishTime(IntentEntry someEntry) {
        int compare = Long.compare(this.published,someEntry.published);
        if (compare == 0)
            compare = tiebreaker(this.member, someEntry.member);
        return compare;
    }

    public boolean isPublishedBefore(IntentEntry someEntry) {
        return comparePublishTime(someEntry) < 0;
    }

    public boolean isPublishedAfter(IntentEntry someEntry) {
        return comparePublishTime(someEntry) > 0;
    }

    public boolean isEnteredBefore(IntentEntry other){
        return this.lastUnlockedOrAcquired < other.lastUnlockedOrAcquired || (this.lastUnlockedOrAcquired == other.lastUnlockedOrAcquired && tiebreaker(this.member, other.member) < 0);
    }

    public boolean isAckedBy(IntentEntry entry){
        return entry.published >= this.lastUnlockedOrAcquired;
    }

    public boolean isAnAcquire(){
        return (state.isAcquiring());
    }
    
    public static int tiebreaker(InetSocketAddress addr1, InetSocketAddress addr2){
        int compPort = Integer.compare(addr1.getPort(),addr2.getPort());
        if (compPort != 0)
            return compPort;

        if (addr1.getAddress() == null){
            if (addr2.getAddress() == null)
                return 0;
            else
                return -1;
        } else if (addr2.getAddress() == null)
            return 1;

        byte[] myBytes = addr1.getAddress().getAddress();
        byte[] theirBytes = addr1.getAddress().getAddress();
        if (myBytes.length < theirBytes.length)
            return -1;
        if (myBytes.length > theirBytes.length)
            return 1;

        for (int i=0;i<myBytes.length;i++){
            int compByte = Byte.compare(myBytes[i],theirBytes[i]);
            if (compByte != 0)
                return compByte;
        }
        return 0;
    }

    public int compareTo(IntentEntry other){
        int compET = Long.compare(this.lastUnlockedOrAcquired,other.lastUnlockedOrAcquired);
        if (compET != 0)
            return compET;

        //tiebreaker.  Only needs to make a given time distinct in a repeatable way, no other significance is assumed.
        return tiebreaker(this.member, other.member);
    }

    public String toString(){
        return String.format("IntentEntry[%s,%s,pub=%s,entry=%s]",member,state,published,lastUnlockedOrAcquired);
    }

}
