package com.tesora.dve.charset;

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

import com.tesora.dve.db.mysql.common.JavaCharsetCatalog;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public interface NativeCharSetCatalog extends JavaCharsetCatalog {
    void addCharSet(NativeCharSet ncs);

    NativeCharSet findCharSetByName(String charSetName);

    NativeCharSet findCharSetByCollation(String collation);

    NativeCharSet findCharSetByCollationId(int collationId);

    int size();

    Set<Map.Entry<String, NativeCharSet>> getCharSetCatalogEntriesByName();

    Set<Map.Entry<String, NativeCharSet>> getCharSetCatalogEntriesByPEName();

    Collection<NativeCharSet> findAllNativeCharsets();

    @Override
    Charset findJavaCharsetById(int clientCharsetId);

    NativeCharSet findNativeCharsetById(int clientCharsetId);

    boolean isCompatibleCharacterSet(String charSet);
}
