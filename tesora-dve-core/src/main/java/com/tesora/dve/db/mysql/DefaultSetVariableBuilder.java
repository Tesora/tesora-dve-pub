package com.tesora.dve.db.mysql;

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

import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.variable.DBSessionVariableHandler;
import com.tesora.dve.variable.SessionVariableHandler;
import com.tesora.dve.variable.VariableConfig;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class DefaultSetVariableBuilder implements SetVariableSQLBuilder {

        Map<String,String> entries = new HashMap<>();
        @Override
        public void add(String key, String value) {
            entries.put(key,value);
        }

        @Override
        public void remove(String key, String previousValue) {
            //ignore for now.
        }

        @Override
        public void update(String key, String previousValue, String newValue) {
            entries.put(key,newValue);
        }

        @Override
        public void same(String key, String sameValue) {
            //ignore.
        }

        @Override
        public SQLCommand generateSql() throws PENotFoundException {
            String setStatement = null;
            VariableConfig<SessionVariableHandler> sessionConfigTemplate = Singletons.require(HostService.class).getSessionConfigTemplate();
            int clause = 0;

            for (Map.Entry<String,String> entry : entries.entrySet()) {
                String variableName = entry.getKey();
                String variableValue = entry.getValue();
                SessionVariableHandler handler;
                if (("@" + DBNative.DVE_SITENAME_VAR).equals(variableName)) //TODO: ugly special case, there is probably a better way to do this. -sgossard
                    handler = new DBSessionVariableHandler();
                else
                    handler = sessionConfigTemplate.getVariableInfo(variableName).getHandler();
                String sessionContextSetting = handler.getSessionAssignmentClause(variableName, variableValue);
                if (sessionContextSetting != null){
                    if (setStatement == null)
                        setStatement = "SET ";
                    setStatement += (clause++ > 0 ? "," : "") + sessionContextSetting;
                }
            }

            if (setStatement == null)
                return SQLCommand.EMPTY;
            else
                return new SQLCommand(setStatement);

        }
}
