// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

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

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;

import java.net.SocketAddress;



public class ChannelHandlerContextStub implements ChannelHandlerContext {

	@Override
	public <T> Attribute<T> attr(AttributeKey<T> key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelPipeline pipeline() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ByteBufAllocator alloc() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelPromise newPromise() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelProgressivePromise newProgressivePromise() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture newSucceededFuture() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture newFailedFuture(Throwable cause) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelPromise voidPromise() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture bind(SocketAddress localAddress) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress,
			SocketAddress localAddress) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture disconnect() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture close() {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("deprecation")
	@Override
	@Deprecated
	public ChannelFuture deregister() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress,
			ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress,
			SocketAddress localAddress, ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture disconnect(ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture close(ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("deprecation")
	@Override
	@Deprecated
	public ChannelFuture deregister(ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandlerContext read() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture write(Object msg) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture write(Object msg, ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture writeAndFlush(Object msg) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Channel channel() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EventExecutor executor() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String name() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandler handler() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isRemoved() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ChannelHandlerContext fireChannelRegistered() {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("deprecation")
	@Override
	@Deprecated
	public ChannelHandlerContext fireChannelUnregistered() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandlerContext fireChannelActive() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandlerContext fireChannelInactive() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandlerContext fireExceptionCaught(Throwable cause) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandlerContext fireUserEventTriggered(Object event) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandlerContext fireChannelRead(Object msg) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandlerContext fireChannelReadComplete() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandlerContext fireChannelWritabilityChanged() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandlerContext flush() {
		// TODO Auto-generated method stub
		return null;
	}
//
//	@Override
//	public <T> Attribute<T> attr(AttributeKey<T> key) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelPipeline pipeline() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ByteBufAllocator alloc() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelPromise newPromise() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelFuture newSucceededFuture() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelFuture newFailedFuture(Throwable cause) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelFuture bind(SocketAddress localAddress) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelFuture connect(SocketAddress remoteAddress) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelFuture connect(SocketAddress remoteAddress,
//			SocketAddress localAddress) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelFuture disconnect() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelFuture close() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelFuture deregister() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelFuture write(Object message) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelFuture connect(SocketAddress remoteAddress,
//			ChannelPromise promise) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelFuture connect(SocketAddress remoteAddress,
//			SocketAddress localAddress, ChannelPromise promise) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelFuture disconnect(ChannelPromise promise) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelFuture close(ChannelPromise promise) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelFuture deregister(ChannelPromise promise) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelFuture write(Object message, ChannelPromise promise) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public Channel channel() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public EventExecutor executor() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public String name() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelHandler handler() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//
//	@Override
//	public ChannelHandlerContext fireChannelRegistered() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelHandlerContext fireChannelUnregistered() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelHandlerContext fireChannelActive() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelHandlerContext fireChannelInactive() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelHandlerContext fireExceptionCaught(Throwable cause) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelHandlerContext fireUserEventTriggered(Object event) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public ChannelProgressivePromise newProgressivePromise() {
//		return null;
//	}
//
//	@Override
//	public ChannelPromise voidPromise() {
//		return null;
//	}
//
//	@Override
//	public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
//		return null;
//	}
//
//	@Override
//	public ChannelFuture writeAndFlush(Object msg) {
//		return null;
//	}
//
//	@Override
//	public boolean isRemoved() {
//		return false;
//	}
//
//	@Override
//	public ChannelHandlerContext fireChannelRead(Object msg) {
//		return null;
//	}
//
//	@Override
//	public ChannelHandlerContext fireChannelReadComplete() {
//		return null;
//	}
//
//	@Override
//	public ChannelHandlerContext fireChannelWritabilityChanged() {
//		return null;
//	}
//
//	@Override
//	public ChannelHandlerContext flush() {
//		return null;
//	}

}
