package com.tesora.dve.server.global;

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

import com.fasterxml.uuid.impl.TimeBasedGenerator;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnectionProxy;
import com.tesora.dve.sql.infoschema.InformationSchema;
import com.tesora.dve.sql.infoschema.InformationSchemaService;
import com.tesora.dve.sql.infoschema.InformationSchemas;
import com.tesora.dve.variables.ScopedVariables;
import com.tesora.dve.variables.VariableManager;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 *
 */
public interface HostService {
    String getBroadcastMessageAgentAddress();

    void setBroadcastMessageAgentAddress(String broadcastMessageAgentAddress);

    String getNotificationManagerAddress();

    void setNotificationManagerAddress(String notificationManagerAddress);

    String getStatisticsManagerAddress();

    void setStatisticsManagerAddress(String statisticsManagerAddress);

    String getWorkerManagerAddress();

    Properties getProperties();

    String getName();

    String getHostName();

    String getDefaultProjectName();

    String getServerVersion();

    String getServerVersionComment();

    String getDveServerVersion();

    String getDveServerVersionComment();

    int getPortalPort(Properties props);

    Collection<String> getScopedVariableScopeNames();

    Map<String, String> getScopedVariables(String scopeName) throws PEException;

    void setScopedVariable(String scopeName, String variableName, String value) throws PEException;

    void addScopedConfig(String scopeName, ScopedVariables config);

    long getDBConnectionTimeout();

    TimeBasedGenerator getUuidGenerator();

    long getServerUptime();

    void resetServerUptime();

    <T> Future<T> submit(Callable<T> task);

    <T> Future<T> submit(String name, Callable<T> task);

    void execute(Runnable task);

    void execute(String name, Runnable task);

    void onGarbageEvent();
    
    VariableManager getVariableManager();
    
    SSConnectionProxy getRootProxy() throws PEException;
}
