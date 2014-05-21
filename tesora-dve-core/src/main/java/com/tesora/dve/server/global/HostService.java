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
import com.tesora.dve.charset.CharSetNative;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.Project;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.infoschema.InformationSchemas;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.variable.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 *
 */
public interface HostService {
    String getBroadcastMessageAgentAddress();

    void setBroadcastMessageAgentAddress(String broadcastMessageAgentAddress);

    String getNotificationManagerAddress();

    void setNotificationManagerAddress(String notificationManagerAddress);

    String getTransactionManagerAddress();

    String getStatisticsManagerAddress();

    void setStatisticsManagerAddress(String statisticsManagerAddress);

    String getWorkerManagerAddress();

    Properties getProperties();

    String getName();

    String getHostName();

    Project getProject();

    void setProject(Project p);

    CharSetNative getCharSetNative();

    String getDefaultProjectName();

    List<Pair<String,String>> getStatusVariables(CatalogDAO c) throws PEException;

    String getStatusVariable(CatalogDAO c, String variableName) throws PEException;

    String getGlobalVariable(CatalogDAO c, String variableName) throws PEException;

    GlobalVariableHandler getGlobalVariableHandler(String variableName) throws PEException;

    void setGlobalVariable(CatalogDAO c, String variableName, String value) throws PEException;

    Map<String,String> getGlobalVariables(CatalogDAO c) throws PEException;

    String getServerVersion();

    String getServerVersionComment();

    String getDveVersion(SSConnection ssCon) throws PEException;

    String getDveServerVersion();

    String getDveServerVersionComment();

    int getPortalPort(Properties props);

    String getScopedVariable(String scopeName, String variableName) throws PEException;

    Collection<String> getScopedVariableScopeNames();

    Map<String, String> getScopedVariables(String scopeName) throws PEException;

    void setScopedVariable(String scopeName, String variableName, String value) throws PEException;

    void addScopedConfig(String scopeName, VariableConfig<ScopedVariableHandler> variableConfig);

    VariableConfig<SessionVariableHandler> getSessionConfigTemplate();

    VariableValueStore getSessionConfigDefaults(SSConnection ssCon) throws PEException;

    InformationSchemas getInformationSchema();

    long getDBConnectionTimeout();

    TimeBasedGenerator getUuidGenerator();

    long getServerUptime();

    void resetServerUptime();

    ExecutorService getExecutorService();

    <T> Future<T> submit(Callable<T> task);

    <T> Future<T> submit(String name, Callable<T> task);

    void execute(Runnable task);

    void execute(String name, Runnable task);

    void onGarbageEvent();

    DBNative getDBNative();
}
