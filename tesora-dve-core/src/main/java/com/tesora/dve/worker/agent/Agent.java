package com.tesora.dve.worker.agent;

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
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.exceptions.PEException;

public abstract class Agent {

	static AtomicLong agentId = new AtomicLong(0);

	protected long exceptionFrame = 0;
	protected String name;

	private static PluginProvider<?> provider;
	
	AgentPlugin plugin;

	private Set<Thread> shutdownHooks = new HashSet<Thread>();
	
	protected Agent() {};
	
	public Agent(String name) throws PEException {
		this.name = name + agentId.incrementAndGet();
		
		initialize();
	}
	
	protected void initialize() throws PEException {
		try {
			plugin = provider.newInstance(this);
		} catch (Exception e) {
			throw new PEException("Unable to instantiate new AgentPlugin", e);
		}
	}

	public static void setPluginProvider(PluginProvider<? extends AgentPlugin> pluginProvider) throws PEException {
		try {
			provider = pluginProvider;
		} catch (Exception e) {
			throw new PEException("Cannot find newInstance() method in AgentPlugin", e);
		}
	}
	
	public static void startServices(Properties props) throws PEException {
		try {
			provider.startServices(props);
		} catch (Exception e) {
			throw new PEException("Unable to start Agent services", e);
		}
	}
	
	public static void stopServices() throws PEException {
		try {
			if(provider != null)
				provider.stopServices();
		} catch (Exception e) {
			throw new PEException("Unable to stop Agent services", e);
		}
	}

	/**
	 * A static method for sending a message without instantiating an Agent
	 * @param e Envelope with the message to send and destination address
	 * @throws PEException 
	 */
	public static void dispatch(String toAddress, Object message) throws PEException {
		try {
			provider.dispatch(toAddress,message);
		} catch (Exception e) {
			throw new PEException("Unable to dispatch envelope", e);
		}
	}

	public void close() throws PEException
	{
		plugin.close();

		// We copy the list in case the hook removes itself
		List<Thread> hooksToRun = new ArrayList<Thread>(shutdownHooks);
		for (Thread shutdownHook : hooksToRun) {
			shutdownHook.run();
		}
	}

	public String getReplyAddress() throws PEException
	{
		return plugin.getReplyAddress();
	}

	public String getAddress() throws PEException
	{
		return plugin.getAddress();
	}

	public void returnResponse(Envelope requestEnvelope, ResponseMessage resp) throws PEException
	{
		plugin.returnResponse(requestEnvelope, resp);
	}

	public ResponseMessage sendAndReceive(Envelope e) throws PEException
	{
		return plugin.sendAndReceive(e);
	}

	public void send(Envelope e) throws PEException
	{
		plugin.send(e);
	}

	public abstract void onMessage(Envelope e) throws PEException;

	public void onTimeout() {
	}

	/**
	 * Utility method to return a random value between 0 and <code>size</code>-1.
	 *  
	 * @param size upper bound on value to be returned
	 * @return random value
	 */
	public static int getRandom(int size) {
		return (int) (Math.random()*size);
	}

	/**
	 * Gets the name of the Agent.  This would be the queue name for Agents such
	 * as the WorkerManager, but is only for convenience and may not follow this 
	 * convention.
	 * 
	 * @return the Agent's name
	 */
	public String getName() {
		return name;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName()+"("+name+")";
	}

	/**
	 * Bumps the Agent's exception frame.  The exception frame is used to silently consume messages
	 * on the Send/Receive/Reply queue after and exception (i.e., outstanding messages from other
	 * workers after one worker returns an exception).
	 */
	public void nextExceptionFrame() {
		++exceptionFrame;
	}

	public long getExceptionFrame() {
		return exceptionFrame;
	}
	
	public Envelope newEnvelope(Object o) {
		return plugin.newEnvelope(o);
	}
	
	public void addShutdownHook(Thread shutdownHook) {
		this.shutdownHooks.add(shutdownHook);
	}
	
	public void removeShutdownHook(Thread shutdownHook) {
		this.shutdownHooks.remove(shutdownHook);
	}
}