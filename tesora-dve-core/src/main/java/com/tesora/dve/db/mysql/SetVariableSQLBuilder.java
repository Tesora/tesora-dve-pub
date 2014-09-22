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

import java.nio.charset.Charset;

import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.server.messaging.SQLCommand;

/**
 *
 */
public interface SetVariableSQLBuilder {

    void add(String key, String value);
    void remove(String key, String previousValue);
    void update(String key, String previousValue, String newValue);
    void same(String key, String sameValue);

	SQLCommand generateSql(Charset connectionCharset) throws PENotFoundException;
}
