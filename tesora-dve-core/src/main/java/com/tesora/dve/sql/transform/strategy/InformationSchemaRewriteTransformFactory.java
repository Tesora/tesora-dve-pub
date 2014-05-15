// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;

import java.util.Collections;

import org.apache.commons.lang.ArrayUtils;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.engine.EntityResults;
import com.tesora.dve.sql.infoschema.engine.LogicalSchemaQueryEngine;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.execution.DDLQueryExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.NonDMLFeatureStep;
import com.tesora.dve.sql.util.ListSet;

public class InformationSchemaRewriteTransformFactory extends TransformFactory {

	// We don't support these yet - just return an empty result set
	static final String[] emptyRSTables = { "plugins", "events", "files", "partitions", "routines" };
	
	public static boolean applies(SchemaContext sc, DMLStatement stmt, boolean hasParent) throws PEException {
		ListSet<TableKey> tables = EngineConstant.TABLES_INC_NESTED.getValue(stmt,sc);
		// so, this transform applies only when any of the tables is an info schema table
		// we throw exceptions for the following causes:
		// mixed info schema and info schema
		// stmt is not a select stmt
		boolean haveInfo = false;
		boolean haveUser = false;
		for(TableKey tk : tables) {
			if (tk.getTable().isInfoSchema())
				haveInfo = true;
			else
				haveUser = true;
			
			if (haveInfo &&
					ArrayUtils.indexOf(emptyRSTables, tk.getTable().getName().getUnqualified().get()) != -1) {
				stmt.getBlock().store(InformationSchemaRewriteTransformFactory.class,Boolean.TRUE);
			}
		}
		if (!haveInfo) return false;
		if (haveInfo && haveUser)
			throw new PEException("Invalid info schema query: mixes info schema tables and user tables");
		if (!(stmt instanceof SelectStatement))
			throw new PEException("Invalid info schema query: not a select statement");
		if (hasParent)
			throw new PEException("No support for nested info schema queries");
		
		return true;
		
	}
	
	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.INFO_SCHEMA;
	}

	private static IntermediateResultSet buildEmptyResultSet(SchemaContext schemaContext, SelectStatement src) {
		EntityResults er = new EntityResults(LogicalSchemaQueryEngine.convertDown(schemaContext, new ViewQuery(
				src, Collections.<String, Object> emptyMap(), null)),
				Collections.<CatalogEntity> emptyList());
		return er.getResultSet(schemaContext);
	} 
	
	@Override
	public FeatureStep plan(final DMLStatement stmt, PlannerContext ipc) throws PEException {
		if (!applies(ipc.getContext(), stmt, !ipc.getApplied().isEmpty()))
			return null;
		Boolean value = (Boolean) stmt.getBlock().getFromStorage(InformationSchemaRewriteTransformFactory.class);
		boolean forceEmptyRS = false;
		if (value != null) {
			stmt.getBlock().clearFromStorage(InformationSchemaRewriteTransformFactory.class);
			forceEmptyRS = value.booleanValue();
		}
		
		FeatureStep root = null;
		
		if (forceEmptyRS) {
			root = new NonDMLFeatureStep(this, null) {

				@Override
				public void scheduleSelf(PlannerContext sc, ExecutionSequence es)
						throws PEException {
					es.append(new DDLQueryExecutionStep("select",
							buildEmptyResultSet(sc.getContext(), (SelectStatement)stmt)));
				}

			};
		} else {
			root = new NonDMLFeatureStep(this, null) {

				@Override
				public void scheduleSelf(PlannerContext sc, ExecutionSequence es)
						throws PEException {
					IntermediateResultSet irs = LogicalSchemaQueryEngine.execute(sc.getContext(),(SelectStatement) stmt);
					es.append(new DDLQueryExecutionStep("select",irs));					
				}

			}.withCachingFlag(false);
		}
		return root;
	}
}
