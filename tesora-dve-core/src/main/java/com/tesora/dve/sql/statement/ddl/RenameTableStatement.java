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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.parser.TranslatorUtils;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;

public class RenameTableStatement extends DDLStatement {
	
	public static final class IntermediateSchema {

		private final Map<Name, PEAbstractTable<?>> tableLookupCache = new HashMap<Name, PEAbstractTable<?>>();
		private final Map<Name, Boolean> tableStateCache = new HashMap<Name, Boolean>();
		private final Map<Name, Name> renameActions = new LinkedHashMap<Name, Name>();

		public Map<Name, Name> getRenameActions() {
			return Collections.unmodifiableMap(this.renameActions);
		}

		public void addRenameAction(final SchemaContext sc, final Pair<Name, Name> namePair) {
			final Pair<Name, Name> qualifiedNamePair = this.convertToQualifiedNames(sc, namePair);
			this.addRenameAction(qualifiedNamePair.getFirst(), qualifiedNamePair.getSecond());
		}

		public int getNumRenameActions() {
			return this.renameActions.size();
		}

		public void buildRenamePairs(final SchemaContext sc, final ListOfPairs<PEAbstractTable<?>, Name> renamePairs,
				final Set<Pair<SchemaCacheKey<?>, InvalidationScope>> uniqueCacheKeys) {
			for (final Map.Entry<Name, Name> renameAction : this.renameActions.entrySet()) {
				final PEAbstractTable<?> sourceTable = this.tableLookupCache.get(renameAction.getValue());
				final Name targetName = renameAction.getKey();
				renamePairs.add(sourceTable, targetName);
				uniqueCacheKeys.add(new Pair<SchemaCacheKey<?>, InvalidationScope>(sourceTable.getDatabase(sc).getCacheKey(), InvalidationScope.CASCADE));
			}
		}

		public void addRenameAction(final Name source, final Name target) {
			if (this.renameActions.containsKey(source)) {
				final Name previous = this.renameActions.remove(source);
				this.renameActions.put(target, previous);
			} else {
				this.renameActions.put(target, source);
			}
		}

		public void clear() {
			this.renameActions.clear();
			this.tableStateCache.clear();
			this.tableLookupCache.clear();
		}

		private Pair<Name, Name> convertToQualifiedNames(final SchemaContext sc, final Pair<Name, Name> namePair) {
			Name source = namePair.getFirst();
			Name target = namePair.getSecond();

			final UnqualifiedName sourceDbName = TranslatorUtils.getDatabaseNameForTable(sc, source);
			final UnqualifiedName targetDbName = TranslatorUtils.getDatabaseNameForTable(sc, target);
			if (!sourceDbName.equals(targetDbName)) {
				/* This is a DVE only limitation. */
				throw new SchemaException(Pass.FIRST, "Moving tables between databases and persistent groups is not allowed.");
			}

			/*
			 * Qualifying the table names should help to avoid future database
			 * lookups.
			 */
			if (!source.isQualified()) {
				source = toQualifiedTableName(sourceDbName, source.getUnqualified());
			}

			if (!target.isQualified()) {
				target = toQualifiedTableName(targetDbName, target.getUnqualified());
			}

			cacheSourceTable(sc, source);
			cacheTargetTable(sc, target);

			return new Pair<Name, Name>(source, target);
		}

		private void cacheSourceTable(final SchemaContext sc, final Name sourceName) {
			if (lookupTable(sc, sourceName)) {
				this.tableStateCache.put(sourceName, false);
			} else {
				throw new SchemaException(Pass.FIRST, "No such table '" + sourceName + "'.");
			}
		}

		private void cacheTargetTable(final SchemaContext sc, final Name targetName) {
			if (!lookupTable(sc, targetName)) {
				this.tableStateCache.put(targetName, true);
			} else {
				throw new SchemaException(Pass.FIRST, "Table '" + targetName + "' already exists.");
			}
		}

		private boolean lookupTable(final SchemaContext sc, final Name tableName) {
			if (this.tableLookupCache.containsKey(tableName)) {
				return this.tableStateCache.get(tableName);
			}

			final PEAbstractTable<?> table = TranslatorUtils.getTable(sc, tableName, new LockInfo(LockType.EXCLUSIVE, "rename table"));
			this.tableLookupCache.put(tableName, table);
			final Boolean exists = (table != null);
			this.tableStateCache.put(tableName, exists);

			return exists;
		}
	}

	public static RenameTableStatement buildRenameTableStatement(final SchemaContext sc, final List<Pair<Name, Name>> sourceTargetNamePairs) {
		assert (!sourceTargetNamePairs.isEmpty());
		
		final IntermediateSchema schema = new IntermediateSchema();
		for (final Pair<Name, Name> namePair : sourceTargetNamePairs) {
			schema.addRenameAction(sc, namePair);
		}

		final int numRenameActions = schema.getNumRenameActions();
		final ListOfPairs<PEAbstractTable<?>, Name> renamePairs = new ListOfPairs<PEAbstractTable<?>, Name>(numRenameActions);
		final Set<Pair<SchemaCacheKey<?>, InvalidationScope>> uniqueCacheKeys = new HashSet<Pair<SchemaCacheKey<?>, InvalidationScope>>(numRenameActions);
		
		schema.buildRenamePairs(sc, renamePairs, uniqueCacheKeys);
		
		final CacheInvalidationRecord invalidationRecord = new CacheInvalidationRecord(uniqueCacheKeys);
		
		return new RenameTableStatement(sourceTargetNamePairs, renamePairs, invalidationRecord);
	}

	private static Name toQualifiedTableName(final UnqualifiedName dbName, final UnqualifiedName tableName) {
		return new QualifiedName(dbName, tableName);
	}

	private static List<CatalogEntity> renameTables(final SchemaContext pc, final List<Pair<PEAbstractTable<?>, Name>> tableNamePairs) throws PEException {
		pc.beginSaveContext(true);
		try {
			for (final Pair<PEAbstractTable<?>, Name> tableNamePair : tableNamePairs) {
				final PEAbstractTable<?> table = tableNamePair.getFirst();
				final Name newName = tableNamePair.getSecond();
				table.persistTree(pc, true);
				table.setName(newName.getUnqualified());
			}
		} finally {
			pc.endSaveContext();
		}
		pc.beginSaveContext(true);
		try {
			for (final Pair<PEAbstractTable<?>, Name> tableNamePair : tableNamePairs) {
				tableNamePair.getFirst().persistTree(pc);
			}

			return Functional.toList(pc.getSaveContext().getObjects());
		} finally {
			pc.endSaveContext();
		}
	}

	private final List<Pair<Name, Name>> sourceTargetNamePairs;
	private final List<Pair<PEAbstractTable<?>, Name>> tableNamePairs;
	private final CacheInvalidationRecord invalidationRecord;

	protected RenameTableStatement(final List<Pair<Name, Name>> sourceTargetNamePairs, final List<Pair<PEAbstractTable<?>, Name>> tableNamePairs,
			final CacheInvalidationRecord invalidationRecord) {
		super(false);

		this.sourceTargetNamePairs = sourceTargetNamePairs;
		this.tableNamePairs = tableNamePairs; 
		this.invalidationRecord = invalidationRecord;
	}

	public List<Pair<Name, Name>> getNamePairs() {
		return Collections.unmodifiableList(this.sourceTargetNamePairs);
	}

	@Override
	public List<CatalogEntity> getCatalogObjects(SchemaContext pc) throws PEException {
		return renameTables(pc, this.tableNamePairs);
	}

	@Override
	public Action getAction() {
		return Action.ALTER;
	}

	@Override
	public Persistable<?, ?> getRoot() {
		return this.tableNamePairs.get(0).getFirst();
	}

	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		return this.invalidationRecord;
	}
}
