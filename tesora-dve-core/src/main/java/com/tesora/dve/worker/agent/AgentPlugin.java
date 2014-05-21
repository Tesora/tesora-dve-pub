// OS_STATUS: public
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

import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.exceptions.PEException;

public interface AgentPlugin {

	/**
	 * onMessage is the method overridden in derived classes to handle incoming messages.
	 * Messages arrive wrapped in an <code>Envelope</code>, which is the type of parameter 
	 * <code>e</code>.
	 * 
	 * @param e Envelope containing the message to send
	 * @throws JMSException
	 * @throws PEException
	 */
//	void onMessage(Envelope e) throws PEException;

	/**
	 * Sends a message to the destination specified in the envelope <code>e</code>
	 * 
	 * @param e Envelope containing the message to send
	 * @throws PEException
	 * @throws PEException
	 */
	void send(Envelope e) throws PEException;

	void sendSynchronous(Envelope envelope) throws PEException;

	/**
	 * Receives a message from the Send/Receive/Respone queue and 
	 * returns it as a <code>ResponseMessage</code>.  If the message
	 * received encodes an exception captured on the remote site,
	 * the exception will be rethrown.
	 * 
	 * @return the <b>ResponseMessage</b> received
	 * @throws PEException
	 * @throws PEException
	 */
	ResponseMessage receive() throws PEException;

	/**
	 * Convenience method that wraps a send and receive.
	 * 
	 * @see JMSAgentPlugin#send
	 * @see JMSAgentPlugin#receive
	 * @param e Envelope containing the message to send
	 * @return ResponseMessage
	 * @throws PEException
	 * @throws PEException
	 */
	ResponseMessage sendAndReceive(Envelope e) throws PEException;

	/**
	 * Retrieves the address used to send messages to this {@code Agent}.
	 * 
	 * @return the <b>Agent</b>'s address
	 * @throws PEException
	 */
	String getAddress() throws PEException;

	/**
	 * Retrieves the address used to send replies to for messages
	 * send from this {@code Agent}.
	 * 
	 * @return the ReplyTo address
	 * @throws PEException
	 */
	String getReplyAddress() throws PEException;

	void close() throws PEException;

	void returnResponse(Envelope requestEnvelope, ResponseMessage resp)
			throws PEException;

	Envelope newEnvelope(Object o);

	public <T> Envelope getNextEnvelopeByType(Class<T> msgType) throws PEException;
	
//	AgentPlugin newInstance(String name) throws PEException;
//
//	AgentPlugin newInstance() throws PEException;
}