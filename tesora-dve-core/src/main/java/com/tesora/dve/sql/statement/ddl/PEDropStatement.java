// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.EmptyExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;

public class PEDropStatement<TransientClass, PersistentClass> extends DropStatement {

	// one of these will be set.
	protected Persistable<TransientClass, PersistentClass> toDrop;
	protected Name nameToDrop;
	protected Class<?> tsc;
	protected String tag;
	
	public PEDropStatement(Class<?> tschemaClass, Boolean ifExists, boolean peOnly, Persistable<TransientClass, PersistentClass> targ, String tag) {
		super(ifExists, peOnly);
		toDrop = targ;
		nameToDrop = targ.getName();
		tsc = tschemaClass;
		this.tag = tag;
	}

	public PEDropStatement(Class<?> tschemaClass, Boolean ifExists, boolean peOnly, Name targ, String tag) {
		super(ifExists, peOnly);
		toDrop = null;
		nameToDrop = targ;
		tsc = tschemaClass;
		this.tag = tag;
	}

	public Persistable<TransientClass, PersistentClass> getTarget() {
		return toDrop;
	}
	
	public Name getTargetName() {
		return nameToDrop;
	}

	public String getSchemaTag() {
		return this.tag;
	}
	
	public Class<?> getTargetClass() {
		return tsc;
	}
	
	@Override
	public List<CatalogEntity> getDeleteObjects(SchemaContext pc) throws PEException {
		if (getTarget() != null) {
			pc.beginSaveContext();
			try {
				return Collections.singletonList((CatalogEntity)getTarget().persistTree(pc));
			} finally {
				pc.endSaveContext();
			}
		}
		else
			return Collections.emptyList();
	}

	@Override
	public Action getAction() {
		return Action.DROP;
	}

	@Override
	public Persistable<?, ?> getRoot() {
		return getTarget();
	}

	@Override
	public void plan(SchemaContext pc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		if (toDrop != null) {
			es.append(buildStep(pc));
		} else {
			es.append(new EmptyExecutionStep(0,"already dropped: " + getSQL(pc)));
		}
	}

	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		if (toDrop != null)
			return new CacheInvalidationRecord(toDrop.getCacheKey(),InvalidationScope.CASCADE);
		return null;
	}
	
}
