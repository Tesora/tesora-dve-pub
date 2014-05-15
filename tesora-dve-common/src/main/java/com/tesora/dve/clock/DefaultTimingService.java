// OS_STATUS: public
package com.tesora.dve.clock;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


public class DefaultTimingService implements TimingService, Closeable {
    private static final ThreadLocal<Timer> boundTimer = new ThreadLocal<>();
    private static final Charset utf8 = Charset.forName("UTF-8");

    enum DefaultLocation{ ROOT }

    final StampClock clock;
    final Timer defaultRootTimer;

    Path perfLogFile;
    PrintWriter outputWriter;
    ScheduledExecutorService scheduledExec;

    public DefaultTimingService(StampClock clock, Path perfLogFile) throws IOException {
        this.perfLogFile = perfLogFile;
        this.clock = clock;
        this.outputWriter = new PrintWriter(Files.newBufferedWriter(perfLogFile,utf8));//default opens for write, truncates existing file.

        DefaultTimer defTime = new DefaultTimer(this.clock, DefaultLocation.ROOT );
        this.defaultRootTimer = defTime;
        defTime.onStart();

        //TODO: since we have a flush thread to keep the trailing bits of logging from getting buffered forever, have this thread do the writes. -sgossard
        scheduledExec = Executors.newScheduledThreadPool(1,new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,"TimingServiceFlush");
            }
        });

        scheduledExec.scheduleWithFixedDelay(
                new Runnable() {
                    public void run() {
                        doOccasionalFlush();
                    }
                },
                0L,
                30L,
                TimeUnit.SECONDS
        );

    }

    protected void doOccasionalFlush() {
        outputWriter.flush();//print writer flush catches IO exceptions, and worst thing that happens is you have to turn logging off to get last few lines.
    }

    @Override
    public void close() throws IOException {
        outputWriter.close(); //print writer traps IOException.
        scheduledExec.shutdown();
        try {
            scheduledExec.awaitTermination(30L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            //ignore, shouldn't happen.
        }
    }

    @Override
    public Timer getTimerOnThread() {
        Timer onThread = boundTimer.get();
        if (onThread == null)
            return needRootTimer();
        else
            return onThread;
    }

    protected Timer needRootTimer() {
        return defaultRootTimer;
    }

    @Override
    public void detachTimerOnThread() {
        boundTimer.remove();
    }

    @Override
    public Timer attachTimerOnThread(Timer parent) {
        Timer existing = boundTimer.get();
        if (parent == null){
            detachTimerOnThread();
        } else {
            boundTimer.set(parent);
        }
        return existing;
    }

    @Override
    public Timer startSubTimer(Enum location) {
        Timer currentContext = getTimerOnThread();

        return currentContext.newSubTimer(location);
    }


    protected class DefaultTimer implements Timer {
        final StampClock clock;
        final boolean isRoot;
        final Timer topParent;
        final Timer parent;
        final GlobalTimestamp localStart;
        final AtomicReference<GlobalTimestamp> localEnd;

        final Enum location;

        protected DefaultTimer(StampClock clock, Enum rootLoc) {
            this.clock = clock;
            this.isRoot = true;
            this.parent = this;
            this.topParent = this;
            this.localStart = clock.nextTimestamp();
            this.localEnd = new AtomicReference<>(null);
            this.location = rootLoc;
        }

        protected DefaultTimer(StampClock clock, Timer topParent, Timer parent, Enum loc) {
            this.clock = clock;
            this.isRoot = false;
            this.parent = parent;
            if (parent.isRoot())
                this.topParent = this;
            else
                this.topParent = topParent;
            this.localStart = clock.nextTimestamp();
            this.localEnd = new AtomicReference<>(null);
            this.location = loc;
        }

        @Override
        public boolean isRoot() {
            return this.isRoot;
        }

        @Override
        public boolean isTopLevel() {
            return !isRoot && this.topParent == this;
        }

        @Override
        public boolean isNested() {
            return !parent.isRoot();//root has self as parent.
        }

        @Override
        public Enum location() {
            return location;
        }


        @Override
        public Timer newSubTimer(Enum location) {
            if (location == null)
                location = this.location();

            DefaultTimer subTime = new DefaultTimer(clock, this.topParent,this, location);
            subTime.onStart();
            return subTime;
        }

        protected void onStart(){
            //TODO: need to switch this off of sout, to file IO in the log directory. -sgossard
            if (isRoot())
                outputWriter.write("#\tROOT\t" + this.localStart + "\n");
        }

        protected void onEnd(String... context){
            GlobalTimestamp localEndedAt = localEnd.get();
            GlobalTimestamp parentStart = topParent.startedAt();
            Duration delayBeforeStart = localStart.delta(parentStart);
            Duration frontendElapsed = localEndedAt.delta(parentStart);
            Duration backendElapsed = localEndedAt.delta(localStart);

            //TODO: need to switch this off of sout, to file IO in the log directory. -sgossard
            StringBuilder builder = new StringBuilder(256);//typical line is well over 100 characters.
            builder.append(this.localStart);
            builder.append('\t');
            builder.append(parentStart);
            builder.append('\t');
            builder.append(delayBeforeStart.convertTo(TimeUnit.MICROSECONDS));
            builder.append('\t');
            builder.append(frontendElapsed.convertTo(TimeUnit.MICROSECONDS));
            builder.append('\t');
            builder.append(backendElapsed.convertTo(TimeUnit.MICROSECONDS));
            builder.append('\t');
            builder.append(location.name());
            if (context != null){
                for (int i=0;i<context.length;i++){
                    builder.append('\t');
                    builder.append(context[i]);
                }
            }
            builder.append('\n');
            outputWriter.write(builder.toString());
        }


        public GlobalTimestamp startedAt() {
            return this.localStart;
        }

        @Override
        public GlobalTimestamp end() {
            GlobalTimestamp nextStamp = this.clock.nextTimestamp();
            if (this.localEnd.compareAndSet(null, nextStamp)) {
                onEnd();
                return nextStamp;
            } else
                return nextStamp;
        }

        @Override
        public GlobalTimestamp end(String... context) {
            GlobalTimestamp nextStamp = this.clock.nextTimestamp();
            if (this.localEnd.compareAndSet(null, nextStamp)) {
                onEnd(context);
                return nextStamp;
            } else
                return nextStamp;
        }

    }



}
