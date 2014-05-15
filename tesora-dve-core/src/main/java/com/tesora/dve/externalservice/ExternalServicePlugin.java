// OS_STATUS: public
package com.tesora.dve.externalservice;


import java.net.InetSocketAddress;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.groupmanager.GroupMembershipListener.MembershipEventType;

public interface ExternalServicePlugin {
	void initialize(ExternalServiceContext ctxt) throws PEException;

	void start() throws PEException;
	
	void stop();
	
	boolean isStarted(); 
	
	String status() throws PEException;
	
	String getName() throws PEException;

	String getPlugin() throws PEException;

	void close();

	void reload() throws PEException;
	
	void restart() throws PEException;

	boolean denyServiceStart(ExternalServiceContext ctxt) throws PEException;
	
	void handleGroupMembershipEvent(MembershipEventType eventType, InetSocketAddress inetSocketAddress);
}
