package com.tesora.dve.smqplugin;

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

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.tesora.dve.concurrent.ChainedNotifier;
import org.apache.log4j.Logger;

import com.tesora.dve.common.RemoteException;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.worker.agent.Envelope;
import com.tesora.dve.worker.agent.Agent;
import com.tesora.dve.worker.agent.AgentPlugin;

public class SimpleMQPlugin implements AgentPlugin {
	
	static Logger logger = Logger.getLogger(Agent.class);
	static final boolean loggerDebug = logger.isDebugEnabled();

	static class SMQEnvelope implements Envelope {
		
		static AtomicLong nextSequence = new AtomicLong(0); 
		
		Object payload;
		String fromAddress;
		String toAddress;
		long exceptionFrame;
		long sequence;
		ChainedNotifier<ResponseMessage> response = new ChainedNotifier<>();
		
		public SMQEnvelope(Object payload) {
			this.payload = payload;
			this.sequence = nextSequence.incrementAndGet();
		}

		public SMQEnvelope(SMQEnvelope requestEnv, Object payload) {
			this.payload = payload;
			this.sequence = requestEnv.sequence;
		}
		
		public long getExceptionFrame() {
			return exceptionFrame;
		}

		public void setExceptionFrame(long sequence) {
			this.exceptionFrame = sequence;
		}

		public String getFromAddress() {
			return fromAddress;
		}

		public String getToAddress() {
			return toAddress;
		}

		@Override
		public Envelope from(String fromAddr) {
			fromAddress = fromAddr;
			return this;
		}

		@Override
		public Envelope to(String toAddr) {
			toAddress = toAddr;
			return this;
		}

		@Override
		public Object getPayload() {
			return payload;
		}
		
		@Override
		public String toString() {
			return display(new StringBuilder()).toString();
		}

		public StringBuilder display(StringBuilder sb) {
			return sb.append("SMQEnvelope{").append("To:").append(getToAddress())
					.append(", From: ").append(getFromAddress())
					.append(", ").append((getPayload() == null)?"<empty payload>":getPayload().toString())
					.append(", Seq: ").append(sequence)
					.append("}");
		}

		@Override
		public String getReplyAddress() {
			return getFromAddress();
		}

		protected void deliverResponse(ResponseMessage msg){
			response.success(msg);
		}

		protected ResponseMessage sync(boolean canInterrupt) throws Exception {
			return response.sync(canInterrupt);
		}
	}
	
	ArrayBlockingQueue<SMQEnvelope> receiveQueue = new ArrayBlockingQueue<SimpleMQPlugin.SMQEnvelope>(100000);
	ArrayBlockingQueue<SMQEnvelope> replyQueue = new ArrayBlockingQueue<SimpleMQPlugin.SMQEnvelope>(2500);
	static ConcurrentHashMap<String, ArrayBlockingQueue<SMQEnvelope>> agentMap = 
			new ConcurrentHashMap<String, ArrayBlockingQueue<SMQEnvelope>>();
	private Agent theAgent;
	private Thread listenerThread;
	boolean markedForTermination = false;
	
	public SimpleMQPlugin(final Agent agent) throws PEException {
		this.theAgent = agent;
		agentMap.put(getAddress(), receiveQueue);
		agentMap.put(getReplyAddress(), replyQueue);
		listenerThread = new Thread(agent.getName()) {
			@Override
			public void run() {
				try {
					while (!markedForTermination) {
						try {
//							agent.onMessage(receiveQueue.take());
							Envelope e = receiveQueue.poll(5, TimeUnit.SECONDS);
							if (e == null)
								agent.onTimeout();
							else 
								agent.onMessage(e);
						} catch (PEException e) {
							if (e.hasCause(InterruptedException.class))
								markForTermination();
							else
								throw new RuntimeException("Unhandled exception received by Agent", e);
						} catch (InterruptedException e) {
							markForTermination();
						}
					}
				} finally {
						receiveQueue.clear();
						replyQueue.clear();
						theAgent = null;
						listenerThread = null;
				}
			}
		};
		listenerThread.start();
	}

	public static AgentPlugin newInstance(Agent theAgent) throws PEException {
		return new SimpleMQPlugin(theAgent);
	}

	static public void startServices(Properties props) throws Exception {
	}
	
	static public void stopServices() throws Exception {
	}

	static public void dispatch(String toAddress, Object message) throws PEException {
		if (false == toAddress.isEmpty()) {
			SMQEnvelope env = (SMQEnvelope) new SMQEnvelope(message).to(toAddress);
			try {
				if (loggerDebug)
					logger.debug(env.display(new StringBuilder(SimpleMQPlugin.class.getSimpleName()).append(" dispatched ")));
				BlockingQueue<SMQEnvelope> destQueue = agentMap.get(env.getToAddress());
				if (destQueue != null) {
					if (false == destQueue.offer(env,5,TimeUnit.MINUTES))
						throw new PEException("Send exceeded timeout: " + env);
				} else {
					if (logger.isDebugEnabled())
						logger.debug(SimpleMQPlugin.class.getSimpleName() + ".dispatch() cannot find destination \"" + env.getToAddress() + "\"");
				}
			} catch (InterruptedException e1) {
				throw new PEException("send operation interrupted", e1);
			}
		}
	}

	@Override
	public void send(Envelope e) throws PEException {
		SMQEnvelope env = (SMQEnvelope) e;
		if (!env.getToAddress().isEmpty()) {
			try {
				if (loggerDebug)
					logger.debug(env.display(new StringBuilder(theAgent.getName()).append(" sent ")));
				env.setExceptionFrame(theAgent.getExceptionFrame());
				BlockingQueue<SMQEnvelope> destQueue = agentMap.get(env.getToAddress());
				if (destQueue != null) {
					if (false == destQueue.offer(env,5,TimeUnit.MINUTES))
						throw new PEException("Send exceeded timeout: " + env);
				} else {
//					if (false == markedForTermination) {
//						throw new PEException(getClass().getSimpleName() + ".send() cannot find destination \"" + env.getToAddress() + "\"");
//					}
					if (logger.isDebugEnabled())
						logger.debug(getClass().getSimpleName() + ".send() cannot find destination \"" + env.getToAddress() + "\"");
				}
			} catch (InterruptedException e1) {
				markForTermination();
				throw new PEException("send operation interrupted", e1);
			}
		}
	}


	private void markForTermination() {
		markedForTermination = true;
	}

	@Override
	public ResponseMessage sendAndReceive(Envelope msg) throws PEException {
		SMQEnvelope env = (SMQEnvelope) msg;
		env.from(getReplyAddress());
		send(env);
		try {
			return env.sync(true);
		} catch (InterruptedException ie){
			markForTermination();
			throw new PEException("receive operation interrupted", ie);
		}catch (PEException e) {
			throw e;
		} catch (Exception e){
			if (e.getCause() instanceof PEException)
				throw (PEException)e.getCause();
			else
				throw new PEException(e);
		}
	}

	@Override
	public String getAddress() throws PEException {
		return theAgent.getName();
	}

	@Override
	public String getReplyAddress() throws PEException {
		return theAgent.getName()+"-Reply";
	}

	@Override
	public void close() throws PEException {
		if (listenerThread != null) {
			agentMap.remove(getAddress());
			agentMap.remove(getReplyAddress());
			// Between the call for
			// markForTermination and the interrupt call the listenerThread can exit and set itself to null
			// on the way out
			Thread t = listenerThread;
			markForTermination();
			if (t != null)
				t.interrupt();
		}
	}

	@Override
	public void returnResponse(Envelope requestEnvelope, ResponseMessage resp)
			throws PEException {
		SMQEnvelope env = (SMQEnvelope) requestEnvelope;
		env.deliverResponse(resp);
	}

	@Override
	public Envelope newEnvelope(Object o) {
		return new SMQEnvelope(o);
	}

}
