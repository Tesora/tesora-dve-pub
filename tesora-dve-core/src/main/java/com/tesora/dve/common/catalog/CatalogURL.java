package com.tesora.dve.common.catalog;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2015 Tesora Inc.
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

import com.tesora.dve.common.PEUrl;
import com.tesora.dve.exceptions.PEException;

import java.util.Properties;

/**
 *
 */
public class CatalogURL {
    /**
     * Clear any existing query and path and append the required connection
     * properties.
     */
    public static PEUrl buildCatalogBaseUrlFrom(final String otherUrl) throws PEException {
        final PEUrl baseUrl = PEUrl.fromUrlString(otherUrl);
        baseUrl.setPath(null);
        baseUrl.clearQuery();
        final Properties connectionSettings = new Properties();

        connectionSettings.put("useUnicode", "true");
        connectionSettings.put("characterEncoding", "utf8");
        connectionSettings.put("connectionCollation", "utf8_general_ci");

        baseUrl.setQueryOptions(connectionSettings);

        return baseUrl;
    }
}
