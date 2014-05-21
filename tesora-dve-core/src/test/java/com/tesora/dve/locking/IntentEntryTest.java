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

import com.tesora.dve.locking.LockMode;
import com.tesora.dve.locking.impl.IntentEntry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IntentEntryTest {
    InetSocketAddress member1 = new InetSocketAddress("localhost",1111);
    InetSocketAddress member2 = new InetSocketAddress("localhost",2222);
    InetSocketAddress member3 = new InetSocketAddress("localhost",3333);
    InetSocketAddress member4 = new InetSocketAddress("localhost",4444);
    InetSocketAddress member5 = new InetSocketAddress("localhost",5555);

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testIsFor() throws Exception {
        InetSocketAddress member1Copy = new InetSocketAddress("localhost",1111);
        InetSocketAddress differentHostSamePort = new InetSocketAddress("127.0.0.2",1111);

        IntentEntry entry = new IntentEntry(member1, LockMode.UNLOCKED,0L,0L);
        assertTrue(entry.isFor(member1));
        assertTrue(entry.isFor(member1Copy));
        assertFalse(entry.isFor(member2));
        assertFalse(entry.isFor(differentHostSamePort));
    }

    @Test
    public void testIsPublishedBeforeAfter() throws Exception {
        IntentEntry one = new IntentEntry(member1,LockMode.UNLOCKED,100L,300L);
        IntentEntry two = new IntentEntry(member2,LockMode.UNLOCKED,200L,200L);
        IntentEntry three = new IntentEntry(member3,LockMode.UNLOCKED,300L,100L);
        //before
        assertFalse(one.isPublishedBefore(one));
        assertTrue(one.isPublishedBefore(two));
        assertTrue(one.isPublishedBefore(three));

        assertFalse(two.isPublishedBefore(one));
        assertFalse(two.isPublishedBefore(two));
        assertTrue(two.isPublishedBefore(three));

        assertFalse(three.isPublishedBefore(one));
        assertFalse(three.isPublishedBefore(two));
        assertFalse(three.isPublishedBefore(three));

        //after
        assertFalse(one.isPublishedAfter(one));
        assertFalse(one.isPublishedAfter(two));
        assertFalse(one.isPublishedAfter(three));

        assertTrue(two.isPublishedAfter(one));
        assertFalse(two.isPublishedAfter(two));
        assertFalse(two.isPublishedAfter(three));

        assertTrue(three.isPublishedAfter(one));
        assertTrue(three.isPublishedAfter(two));
        assertFalse(three.isPublishedAfter(three));
    }

    @Test
    public void testBeforeAfterSameTimestampDifferentMembers() throws Exception {
        IntentEntry one = new IntentEntry(member1,LockMode.UNLOCKED,200L,100L);
        IntentEntry two = new IntentEntry(member2,LockMode.UNLOCKED,200L,100L);

        //before
        assertTrue(one.isPublishedBefore(two));
        assertFalse(two.isPublishedBefore(one));

        //after
        assertFalse(one.isPublishedAfter(two));
        assertTrue(two.isPublishedAfter(one));
    }

    @Test
    public void testIsAckedBy() throws Exception {
        IntentEntry one   = new IntentEntry(member1,LockMode.UNLOCKED,250L,100L);
        IntentEntry two   = new IntentEntry(member2,LockMode.UNLOCKED,200L,200L);
        IntentEntry three = new IntentEntry(member3,LockMode.UNLOCKED,300L,300L);

        assertTrue(one.isAckedBy(one));
        assertTrue(one.isAckedBy(two));
        assertTrue(one.isAckedBy(three));

        assertTrue(two.isAckedBy(one));
        assertTrue(two.isAckedBy(two));
        assertTrue(two.isAckedBy(three));

        assertFalse(three.isAckedBy(one));
        assertFalse(three.isAckedBy(two));
        assertTrue(three.isAckedBy(three));
    }

    @Test
    public void testIsAnAcquire() throws Exception {
        assertFalse(new IntentEntry(member1, LockMode.UNLOCKED, 0L, 0L).isAnAcquire());
        assertFalse(new IntentEntry(member1,LockMode.SHARED,0L,0L).isAnAcquire());
        assertFalse(new IntentEntry(member1,LockMode.EXCLUSIVE,0L,0L).isAnAcquire());
        assertTrue(new IntentEntry(member1, LockMode.ACQUIRING_SHARED, 0L, 0L).isAnAcquire());
        assertTrue(new IntentEntry(member1, LockMode.ACQUIRING_EXCLUSIVE, 0L, 0L).isAnAcquire());
        assertTrue(new IntentEntry(member1, LockMode.ACQUIRING_EXCLUSIVE_WITH_SHARE, 0L, 0L).isAnAcquire());
    }

    @Test
    public void testCompareTo() throws Exception {
        IntentEntry one = new IntentEntry(member1,LockMode.UNLOCKED,100L,500L);
        IntentEntry two = new IntentEntry(member2,LockMode.UNLOCKED,200L,400L);
        IntentEntry three = new IntentEntry(member3,LockMode.UNLOCKED,300L,300L);
        IntentEntry four = new IntentEntry(member3,LockMode.UNLOCKED,400L,200L);
        IntentEntry five = new IntentEntry(member3,LockMode.UNLOCKED,500L,100L);
        List<IntentEntry> sortedByUpdateOrAcquire = Arrays.asList(three,two,four,five,one);
        Collections.sort(sortedByUpdateOrAcquire);
        assertEquals(Arrays.asList(five,four,three,two,one),sortedByUpdateOrAcquire);
    }
}
