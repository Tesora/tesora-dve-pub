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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;

public class AlterDatabaseStatement extends PEAlterStatement<PEDatabase> {

	private final String charSet;
	private final String collation;

	public AlterDatabaseStatement(final PEDatabase db, final String charSet, final String collation) {
		super(db, false);
		this.charSet = charSet;
		this.collation = collation;
	}

	public String getCharSet() {
		return this.charSet;
	}

	public String getCollation() {
		return this.collation;
	}

	@Override
	protected PEDatabase modify(final SchemaContext pc, final PEDatabase target) throws PEException {
		target.setCharSet(this.charSet);
		target.setCollation(this.collation);

		return target;
	}

	/**
	 * If you change the default character set or collation for a database,
	 * stored routines that use the database defaults must be dropped and
	 * recreated so that they use the new defaults.
	 */
	@Override
	public CacheInvalidationRecord getInvalidationRecord(final SchemaContext sc) {
		return new CacheInvalidationRecord(this.getTarget().getCacheKey(), InvalidationScope.CASCADE);
	}

}
