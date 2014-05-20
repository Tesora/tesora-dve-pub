// OS_STATUS: public
/*
 * Heavily modified JMH microbench provided by Netty under the following license:
 *
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.tesora.netty;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.openjdk.jmh.annotations.*;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 *  Quick test that can be used to micro-bench the various netty allocator options with our default alloc of 512 bytes, at varied thread counts.
 */
@Warmup(iterations = 20)
@Measurement(iterations = 20)
@Fork(1)
@State(Scope.Thread)
public class AllocBenchmark {

    public static void main(String[] args) throws Exception{
        //nice for running in an IDE.  From the command line, it is easier to run via the JMH runner so you can override JMH settings.
        // IE:  java -jar target/microbenchmarks.jar '.*AllocBenchmark.*' -t 4

        ChainedOptionsBuilder include = new OptionsBuilder()
                .include(".*" + AllocBenchmark.class.getSimpleName() + ".*");
        Options opt = include.build();
        new Runner(opt).run();
    }

    private final ByteBufAllocator unpooledHeapAllocator = new UnpooledByteBufAllocator(false);
    private final ByteBufAllocator unpooledDirectAllocator = new UnpooledByteBufAllocator(true);
    private final ByteBufAllocator pooledHeapAllocator = new PooledByteBufAllocator(false);
    private final ByteBufAllocator pooledDirectAllocator = new PooledByteBufAllocator(true);

    @Param({ "512" })
    public int size;

    @GenerateMicroBenchmark
    public void unpooledHeapAllocAndFree() {
        ByteBuf buffer = unpooledHeapAllocator.buffer(size);
        buffer.release();
    }

    @GenerateMicroBenchmark
    public void unpooledDirectAllocAndFree() {
        ByteBuf buffer = unpooledDirectAllocator.buffer(size);
        buffer.release();
    }

    @GenerateMicroBenchmark
    public void pooledHeapAllocAndFree() {
        ByteBuf buffer = pooledHeapAllocator.buffer(size);
        buffer.release();
    }

    @GenerateMicroBenchmark
    public void pooledDirectAllocAndFree() {
        ByteBuf buffer = pooledDirectAllocator.buffer(size);
        buffer.release();
    }
}
