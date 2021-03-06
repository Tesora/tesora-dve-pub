package com.tesora.dve.db.mysql.portal.protocol;

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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteOrder;

public abstract class BaseMSPMessage<S> implements MSPMessage {
    public static final int INITIAL_CAPACITY = 256;

    private S state;
    private ByteBuf buffer;

    public BaseMSPMessage(){
        this.state = null;
        this.buffer = null;
    }

    public BaseMSPMessage(S state){
        this.set(state);
    }

    public BaseMSPMessage(byte[] heapData){
        this.set(Unpooled.wrappedBuffer(heapData));
    }

    public BaseMSPMessage(ByteBuf buffer){
        this.set(buffer);
    }

    public BaseMSPMessage(S state, ByteBuf buffer){
        this.set(state,buffer);
    }

    @Override
    public abstract byte getMysqlMessageType();

    @Override
    public abstract MSPMessage newPrototype(ByteBuf source);

    protected S unmarshall(ByteBuf source) {
        throw new UnsupportedOperationException();
    }

    protected void marshall(S state, ByteBuf destination) {
        throw new UnsupportedOperationException();
    }

    public void marshallPayload(ByteBuf destination){
        ByteBuf sliceContents = readBuffer().slice();
        destination.writeBytes(sliceContents);
    }

    protected S readState() {
        if (isStateSet())
            return this.state;

        if (isBufferSet()){
            S newState = this.unmarshall(this.buffer.slice().order(ByteOrder.LITTLE_ENDIAN));
            this.set(newState,this.buffer);
            return this.state;
        }

        throw new IllegalStateException(String.format("Cannot access state of %s, no fields or buffer provided.",this.getClass().getSimpleName()));
    }

    protected ByteBuf readBuffer() {
        if (isBufferSet())
            return this.buffer;

        if (isStateSet()){
            ByteBuf container = Unpooled.buffer(INITIAL_CAPACITY).order(ByteOrder.LITTLE_ENDIAN);
            marshall(this.state,container);
            this.set(state,container);
            return this.buffer;
        }

        throw new IllegalStateException(String.format("Cannot access buffer of %s, no fields or buffer provided.",this.getClass().getSimpleName()));
    }

    public String toString(){
        return String.format("%s[buffer.length=%s]",this.getClass().getSimpleName(),buffer.readableBytes());
    }

    private void set(ByteBuf buffer) {
        this.set(this.state,buffer);
    }

    protected void set(S state) {
        this.set(state,this.buffer);
    }

    private void set(S state, ByteBuf buffer) {
        this.state = state;
        if (this.buffer != buffer){
            if (this.buffer != null)
                this.buffer.release();
            this.buffer = buffer;
        }
    }

    public boolean isStateSet(){
        return this.state != null;
    }

    public boolean isBufferSet(){
        return this.buffer != null;
    }

    @Override
    public int refCnt() {
        if (buffer != null)
            return buffer.refCnt();
        else
            return 0;
    }

    @Override
    public BaseMSPMessage<S> retain() {
        if (buffer != null)
            buffer.retain();
        return this;
    }

    @Override
    public BaseMSPMessage<S> retain(int increment) {
        if (buffer != null)
            buffer.retain(increment);
        return this;
    }

    @Override
    public boolean release() {
        if (buffer != null)
            return buffer.release();
        else
            return true;
    }

    @Override
    public boolean release(int decrement) {
        if (buffer != null)
            return buffer.release(decrement);
        else
            return true;
    }

}
