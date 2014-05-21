package com.tesora.dve.sql.statement.ddl;

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

import java.util.Iterator;
import java.util.List;

import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.parser.TranslatorUtils;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PESiteInstance;
import com.tesora.dve.sql.schema.PEStorageSite;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.util.Pair;

public class PECreateStorageSiteStatement extends
		PECreateStatement<PEStorageSite, PersistentSite> {

	public PECreateStorageSiteStatement(PEStorageSite newSite) {
		super(newSite, true, TranslatorUtils.PERSISTENT_SITE_TAG, false);
	}

	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		return new CacheInvalidationRecord(getCreated().getCacheKey(), InvalidationScope.CASCADE);
	}

	public static PECreateStorageSiteStatement build(SchemaContext sc, Name name, List<Pair<Name,LiteralExpression>> opts) {
		// we already validated the name - just need to get the options
		String[] popts = consumeCommonOptions(sc,opts, true);
		return new PECreateStorageSiteStatement(new PEStorageSite(sc,name,popts[0],popts[1],popts[2]));
	}

	public static String[] consumeCommonOptions(SchemaContext sc, List<Pair<Name,LiteralExpression>> opts, boolean reqd) {
		String url = null;
		String username =null;
		String password = null;
		for(Iterator<Pair<Name,LiteralExpression>> iter = opts.iterator(); iter.hasNext();) {
			Pair<Name,LiteralExpression> p = iter.next();
			String n = p.getFirst().getUnquotedName().get();
			if (PESiteInstance.OPTION_URL.equalsIgnoreCase(n)) {
				url = p.getSecond().asString(sc);
				iter.remove();
			} else if (PESiteInstance.OPTION_USER.equalsIgnoreCase(n)) {
				username = p.getSecond().asString(sc);
				iter.remove();
			} else if (PESiteInstance.OPTION_PASSWORD.equalsIgnoreCase(n)) {
				password = p.getSecond().asString(sc);
				iter.remove();
			}
		}
		if (reqd) {
			if (url == null)
				throw new SchemaException(Pass.SECOND, "The url option must be specified");
			if (username == null)
				throw new SchemaException(Pass.SECOND, "The username option must be specified");
			if (password == null)
				throw new SchemaException(Pass.SECOND, "The password option must be specified");
		}
		return new String[] { url, username, password };
	}
	
	
}
