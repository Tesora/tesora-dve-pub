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

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.tesora.dve.clock.WalltimeNanos;
import com.tesora.dve.debug.DebugHandle;
import com.tesora.dve.debug.Debuggable;
import com.tesora.dve.debug.StringDebugger;
import com.tesora.dve.groupmanager.CoordinationServices;
import com.tesora.dve.groupmanager.HazelcastGroupTopic;
import com.tesora.dve.groupmanager.HazelcastMembershipListener;
import com.tesora.dve.groupmanager.MembershipView;
import com.tesora.dve.groupmanager.MembershipViewSource;
import com.tesora.dve.groupmanager.SimpleMembershipView;
import com.tesora.dve.locking.ClusterLock;
import com.tesora.dve.lockmanager.LockClient;

public class QuickTester {

    AtomicReference<Thread> exclusiveOwner = new AtomicReference<Thread>(null);
    AtomicInteger readerCount = new AtomicInteger(0);

    TotalLockTime readTimes = new TotalLockTime();
    TotalLockTime writeTimes = new TotalLockTime();
    AtomicLong errorCount = new AtomicLong(0L);

    String lockName = "someGlobalLock";
    int numberOfHazels = 10;
    int millisBetweenStarts = 500;
    int accountCount = 10;
    long amountEach = 1000;
    long amountTotal = accountCount * amountEach;
    int threadsPerHazel = 10;
    int callsPerThread = 200000;
    double readWriteRatio = 0.9999d;
    int READ_SLEEP_MILLIS = 0;

    boolean runWhileAddingHZs = true;
    boolean haltOnExit = true;
    AtomicBoolean dumpOnExit = new AtomicBoolean(true);

    ArrayList<HazelcastInstance> hzEntries = new ArrayList<HazelcastInstance>();
    ArrayList<CoordinationServices> coordinators = new ArrayList<CoordinationServices>();
    ArrayList<ClusterLockManager> lockMgrs = new ArrayList<ClusterLockManager>();

    ArrayList<ClusterLock> locks = new ArrayList<ClusterLock>();

    ExecutorService[] execs = new ExecutorService[numberOfHazels];
    {
        for (int i =0; i < numberOfHazels;i++){
            execs[i] = Executors.newFixedThreadPool( threadsPerHazel );
        }
    }

    final long[] accounts = new long[accountCount];
    {
        for (int i=0;i< accountCount;i++)
        accounts[i] = amountEach;
    }




    public Void call() throws Exception {
        Runtime.getRuntime().addShutdownHook( new Thread() {
            public void run(){
                System.out.println("$$$$$$$$");
                if (dumpOnExit.get()){
                    dumpAllLocks();
                }
            }
        });

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch((threadsPerHazel * numberOfHazels) );

        Config cfg = new Config();
        cfg.setInstanceName("hz1");
        String clusterName = UUID.randomUUID().toString();
        cfg.getGroupConfig().setName(clusterName);
        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(cfg);

        long start = System.currentTimeMillis();

        hzEntries.add(hz1);
        HazelcastGroupTopic<ClusterLockManager.ClusterLockMessage> topic = new HazelcastGroupTopic<ClusterLockManager.ClusterLockMessage>(hz1, "pe.cluster.locking");
        ClusterLockManager clm1 = new ClusterLockManager(
                new WalltimeNanos(),
                generateViewSource(hz1.getCluster().getLocalMember().getInetSocketAddress(),hz1,hz1),
                topic
        );
        hz1.getCluster().addMembershipListener(new HazelcastMembershipListener(clm1));
        topic.addMessageListener(clm1);
        clm1.onStart();
        lockMgrs.add(clm1);

        if (runWhileAddingHZs){
            start = System.currentTimeMillis();
            startHammerOn(clm1,execs[0],startLatch,finishLatch);
            startLatch.countDown();
        }

        for (int i=1;i<numberOfHazels;i++){
            if (runWhileAddingHZs){
                dumpQuickLockCount();
                TimeUnit.MILLISECONDS.sleep(millisBetweenStarts);
            }

            String hzName = "hz"+ (i+1);
            System.out.println("Adding new Hazelcast member, "+hzName);
            dumpQuickLockCount();
            InetSocketAddress addr = hz1.getCluster().getLocalMember().getInetSocketAddress();
            cfg.setInstanceName(hzName);
            cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
            cfg.getNetworkConfig().getJoin().getTcpIpConfig().addAddress(new Address(addr));
            final HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
            hzEntries.add(hz);
            ClusterLockManager clm = new ClusterLockManager(
                    new WalltimeNanos(),
                    generateViewSource(hz.getCluster().getLocalMember().getInetSocketAddress(),hz1,hz),
                    new HazelcastGroupTopic<ClusterLockManager.ClusterLockMessage>(hz,"pe.cluster.locking")
            );
            hz.getCluster().addMembershipListener(new HazelcastMembershipListener(clm));
            topic.addMessageListener(clm);
            clm.onStart();

            lockMgrs.add(clm);

            if (runWhileAddingHZs){
                startHammerOn(clm,execs[i],startLatch,finishLatch);
                dumpQuickLockCount();
            }
        }

        if (!runWhileAddingHZs){
            System.out.println("Main thread, waiting 5 sec before releasing the hounds.");
            TimeUnit.SECONDS.sleep(5);
            System.out.println("Main thread, released the hounds.");
            start = System.currentTimeMillis();
            startLatch.countDown();
            for (int i=0;i<numberOfHazels;i++){
                System.out.println("Starting new load against hz" + (i +1) );
                TimeUnit.MILLISECONDS.sleep(millisBetweenStarts);
                startHammerOn(lockMgrs.get(i),execs[i],startLatch,finishLatch);
            }
        }


        System.out.println("Main thread waiting for finish.");
        for (;;){
                if (finishLatch.await(5,TimeUnit.SECONDS))
                    break;
                else
                    dumpQuickLockCount();
        }
        long finished = System.currentTimeMillis();

        System.out.println("Main thread returned.");
        long total = 0L;
        for (int i=0;i < accounts.length;i++){
            total += accounts[i];
        }
        System.out.printf("Total number of elapsed millis = %s\n", (finished - start));
        System.out.println();
        StringDebugger.output(System.out, "write locks", writeTimes);
        System.out.println();
        StringDebugger.output(System.out,"read locks", readTimes);
        System.out.println();
        System.out.printf("Total locks = %s\n", writeTimes.getLockCount() + readTimes.getLockCount() );
        System.out.printf("Total errors = %s\n", errorCount.get() );
        System.out.printf("Total in accounts is $%s, same? %s\n", total, (total == amountTotal));

        dumpOnExit.set(false);

        return null;
    }

    private MembershipViewSource generateViewSource(final InetSocketAddress addr, final HazelcastInstance trueView, final HazelcastInstance currentView) {
        return new MembershipViewSource() {
            @Override
            public MembershipView getMembershipView() {
                List<InetSocketAddress> trueAddresses = new ArrayList<InetSocketAddress>();
                for (Member memb : trueView.getCluster().getMembers()){
                    trueAddresses.add(memb.getInetSocketAddress());
                }
                List<InetSocketAddress> currentAddresses = new ArrayList<InetSocketAddress>();
                for (Member memb : currentView.getCluster().getMembers()){
                    currentAddresses.add(memb.getInetSocketAddress());
                }
                return SimpleMembershipView.buildView(addr, trueAddresses, currentAddresses);
            }
        };
    }

    private void dumpAllLocks() {
        for (int i=0;i<locks.size();i++){
            System.out.println("******");
            StringDebugger.output(System.out, "lock-" + i, locks.get(i));
            System.out.println("******");
        }
        System.out.flush();
    }

    private void dumpQuickLockCount() {
        System.out.printf("%s, writeLocks=%s, readlocks=%s\n", new Date(),writeTimes.getLockCount(),readTimes.getLockCount());
    }

    private void startHammerOn(ClusterLockManager lockMgr, ExecutorService exec, CountDownLatch startLatch, CountDownLatch finishLatch) {
        ClusterLock lock = lockMgr.generateLockEngine(lockName);
//        ClusterLock5 lock = new ClusterLock5(hz.getName(), coordServices);
        locks.add(lock);

        for (int i=0;i< threadsPerHazel;i++){
            exec.submit( new HammerRun(accounts,lock,callsPerThread,startLatch,finishLatch, readWriteRatio) );
        }
    }

    public static void main(String[] args) throws Exception {
        new QuickTester().call();
        System.exit(1);
    }

    public class HammerRun implements Runnable {
        LockClient client;
        long[] accounts;
        ClusterLock lock;
        Random rand;
        int loopCount;
        double readWriteRatio;

        CountDownLatch startLatch;
        CountDownLatch finishLatch;

        public HammerRun(long[] accounts, ClusterLock lock, int loopCount, CountDownLatch start, CountDownLatch finished, double readWriteRatio) {
            this.accounts = accounts;
            this.lock = lock;
            this.loopCount = loopCount;
            this.rand = new Random();
            this.startLatch = start;
            this.finishLatch = finished;
            this.readWriteRatio = readWriteRatio;

        }

        @Override
        public void run() {
            this.client  = new DumbLockClient(Thread.currentThread().getName());
            try{
                startLatch.await();
                for (int i=0;i< loopCount;i++){
                    double next0to1 = rand.nextDouble();
                    if (Double.compare(next0to1, readWriteRatio) < 0 )
                        checkMoney(i);
                    else
                        moveMoney(i);
                }
            } catch (Throwable e) {
                e.printStackTrace();
                errorCount.getAndIncrement();
                if (haltOnExit)
                    System.exit(1000);
            } finally {
                finishLatch.countDown();
            }
        }

        private void checkMoney(int loop) {
            LockTime time = new LockTime();
            time.acquireStart();
            lock.sharedLock(client,"read lock for checkMoney");
            time.acquireEnd();
            readerCount.incrementAndGet();
            try{
                Thread copyOwner = exclusiveOwner.get();
                if (copyOwner != null){
                    StringDebugger.output(System.out,"*FAIL*",lock);
                    String message = Thread.currentThread() + ", got a shared lock, but " + copyOwner + " is marked as the current exclusive owner (read/write conflict) ";
                    System.out.println(message);
                    dumpAllLocks();
                    throw new IllegalStateException(message);
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(READ_SLEEP_MILLIS);
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }

//                System.out.printf("%s, acquired read lock, processing loop %s\n",Thread.currentThread(),loop);
                long total = 0;
                for (int i=0;i < accounts.length;i++){
                    total += accounts[i];
                }
//                System.out.printf("%s on loop %s calculated total of amount, %s\n", Thread.currentThread(), loop, total);
            } finally {
//                System.out.printf("%s, releasing read lock, processing loop %s\n",Thread.currentThread(),loop);

                readerCount.decrementAndGet();
                time.releaseStart();
               lock.sharedUnlock(client, "read unlock for checkMoney");
                time.releaseEnd();
                readTimes.register(time);
            }

        }

        private void moveMoney(int i) {
            LockTime time = new LockTime();
            time.acquireStart();
            lock.exclusiveLock(client, "write lock for moveMoney");
            time.acquireEnd();
            try{
                Thread copyOwner = exclusiveOwner.get();
                if (copyOwner != null || !exclusiveOwner.compareAndSet(null,Thread.currentThread())){
                    String message = Thread.currentThread() + ", got an exclusive lock, but " + copyOwner + " is marked as current exclusive owner (write/write conflict) ";
                    StringDebugger.output(System.out,"*FAIL*",lock);
                    System.out.println(message);
                    dumpAllLocks();
                    throw new IllegalStateException(message);
                }

                int markedReaders = readerCount.get();
                if (markedReaders != 0){
                    StringDebugger.output(System.out,"*FAIL*",lock);
                    String message = Thread.currentThread() + ", got an exclusive lock, but marked reader count is " + markedReaders + " (read/write conflict)";
                    System.out.println(message);
                    dumpAllLocks();
                    throw new IllegalStateException(message);
                }

//                System.out.printf("%s, acquired lock, processing loop %s\n",Thread.currentThread(),i);
                int firstAccount = rand.nextInt(accounts.length);
                int secondAccount = rand.nextInt(accounts.length);
                long present = accounts[firstAccount];
                if (present < 0){
                    System.err.printf("Account dump ==> %s \n", Arrays.toString( accounts ) );
                    throw new IllegalStateException("account contained negative dollars!");
                }
                long subChunk = rand.nextInt((int) present);
                accounts[firstAccount] -= subChunk;
                accounts[secondAccount] += subChunk;
            } finally {
//                System.out.printf("%s, releasing lock, processing loop %s\n",Thread.currentThread(),i);
                if (!exclusiveOwner.compareAndSet(Thread.currentThread(),null)){
                    StringDebugger.output(System.out,"*FAIL*",lock);
                    String message = Thread.currentThread() + ", trying to unmark exclusive owner, but someone has changed it (write/write conflict)";
                    System.out.println(message);
                    dumpAllLocks();
                    throw new IllegalStateException(message);
                }
                time.releaseStart();
                lock.exclusiveUnlock(client, "write unlock for moveMoney");
                time.releaseEnd();
                writeTimes.register(time);
            }
        }
    }

    static class TotalLockTime implements Debuggable {
        AtomicLong lockCount = new AtomicLong(0L);
        AtomicLong acquireNanos = new AtomicLong(0L);
        AtomicLong heldNanos = new AtomicLong(0L);
        AtomicLong releaseNanos = new AtomicLong(0L);

        void register(LockTime time){
            lockCount.incrementAndGet();
            acquireNanos.addAndGet( time.acquireEnd - time.acquireStart);
            heldNanos.addAndGet( time.releaseStart - time.acquireEnd);
            releaseNanos.addAndGet( time.releaseEnd - time.releaseStart);
        }

        public long getLockCount(){
            return lockCount.get();
        }

        @Override
        public void writeTo(DebugHandle survey) {
            survey.entry("lock count", this.getLockCount());
            if (this.getLockCount() > 0){
                survey.entry("Accumulated acquire time μs", TimeUnit.NANOSECONDS.toMicros(this.acquireNanos.get()));
                survey.entry("Accumulated hold time μs", TimeUnit.NANOSECONDS.toMicros(this.heldNanos.get()));
                survey.entry("Accumulated release time μs", TimeUnit.NANOSECONDS.toMicros(this.releaseNanos.get()));
                survey.line("");
                survey.entry("Average time acquiring μs", TimeUnit.NANOSECONDS.toMicros(this.acquireNanos.get() / this.lockCount.get()));
                survey.entry("Average time held μs", TimeUnit.NANOSECONDS.toMicros(this.heldNanos.get() / this.lockCount.get()));
                survey.entry("Average time releasing μs", TimeUnit.NANOSECONDS.toMicros(this.releaseNanos.get() / this.lockCount.get()));
            }
        }
    }

    static class DumbLockClient implements LockClient {
        String name;

        DumbLockClient(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        public String toString(){
            return name;
        }
    }

    static class LockTime {
        private long acquireStart;
        private long acquireEnd;
        private long releaseStart;
        private long releaseEnd;

        public void acquireStart(){
            acquireStart = System.nanoTime();
        }

        public void acquireEnd(){
            acquireEnd = System.nanoTime();
        }

        public void releaseStart(){
            releaseStart = System.nanoTime();
        }

        public void releaseEnd(){
            releaseEnd = System.nanoTime();
        }
    }



}
