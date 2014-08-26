package com.tesora.dve.sql.statement;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import com.tesora.dve.common.PECharsetUtils;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.db.Emitter;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.db.mysql.MysqlEmitter;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.show.ShowInformationSchemaTable;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.MigrationException;
import com.tesora.dve.sql.node.StatementNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.Parameter;
import com.tesora.dve.sql.parser.PlanningResult;
import com.tesora.dve.sql.parser.PreparePlanningResult;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.ExplainOptions;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.CachedPreparedStatement;
import com.tesora.dve.sql.schema.cache.PlanCacheKey;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.PrePlanner;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.EmptyExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.PrepareExecutionStep;
import com.tesora.dve.sql.util.Cast;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryPredicate;

// base class of all sql statements
public abstract class Statement extends StatementNode {

	// putting all the edge names into Statement so we have a single place to ref them from in the grammar
	public static final String COLUMNSPEC_ATTRIBUTE = "columnspec";
	
	// this is not an edge name, we just put it here for convenience
	public static final String SELECT_LOCK_ATTRIBUTE = "locktype";
		
	protected ExplainOptions explain = null;
			
	public Statement(SourceLocation location) {
		super(location);
	}
		
	public boolean isExplain() {
		return explain != null;
	}
	
	public void setExplain(ExplainOptions opts) {
		explain = opts;
	}
		
	public ExplainOptions getExplain() {
		return explain;
	}
	
	public String getSQL(SchemaContext sc, Emitter emitter, EmitOptions opts, boolean preserveParamMarkers) {
		GenericSQLCommand gsql = getGenericSQL(sc,emitter,opts);
		return gsql.resolve(sc, preserveParamMarkers, (opts == null ? null : opts.getMultilinePretty())).getUnresolved(); 
	}
	
	public String getSQL(SchemaContext sc, boolean withExtensions, boolean preserveParamMarkers) {
		return getSQL(sc,withExtensions,preserveParamMarkers,null);
	}
	
	public String getSQL(SchemaContext sc, boolean withExtensions, boolean preserveParamMarkers, String prettyIndent) {
		EmitOptions opts = null;
		if (withExtensions)
			opts = EmitOptions.PEMETADATA;
		if (prettyIndent != null)
			opts = (opts == null ? EmitOptions.NONE : opts).addMultilinePretty(prettyIndent);
        return getSQL(sc,  
        		(sc.getOptions().isInfoSchemaView() ? new MysqlEmitter() :
        		Singletons.require(HostService.class).getDBNative().getEmitter()), opts, preserveParamMarkers);
	}
		
	public String getSQL(SchemaContext sc, EmitOptions opts, boolean preserveParamMarkers) {
		if (opts == null) return getSQL(sc);
        return getSQL(sc, Singletons.require(HostService.class).getDBNative().getEmitter(), opts, preserveParamMarkers);
	}
	
	public String getSQL(SchemaContext sc) {
		return getSQL(sc,null);
	}
	
	public String getSQL(SchemaContext sc, String prettyIndent) {
		return getSQL(sc,false,false, prettyIndent);
	}

	public SQLCommand getSQLCommand(SchemaContext sc) {
		return getGenericSQL(sc,false,false).getSQLCommand();
	}
	
	public GenericSQLCommand getGenericSQL(SchemaContext sc, Emitter emitter, EmitOptions inopts) {
		EmitOptions opts = inopts;
		if (opts == null)
			opts = EmitOptions.GENERIC_SQL;
		else
			opts = opts.addGenericSQL();		
		emitter.setOptions(opts);
		emitter.startGenericCommand();
		StringBuilder buf = new StringBuilder();
		try {
			if (sc != null)
				emitter.pushContext(sc.getTokens());
			emitter.emitStatement(sc,this, buf);
		} finally {
			if (sc != null)
				emitter.popContext();
		}
		return emitter.buildGenericCommand(buf.toString());
	}
	
	public GenericSQLCommand getGenericSQL(SchemaContext sc, boolean withExtensions, boolean withPretty) {
		EmitOptions opts = (withExtensions ? EmitOptions.PEMETADATA : null);
		if (withPretty) {
			if (opts == null) opts = EmitOptions.NONE.addMultilinePretty("  ");
			else opts = opts.addMultilinePretty("  ");
		}
        return getGenericSQL(sc, Singletons.require(HostService.class).getDBNative().getEmitter(), opts);
	}
	


	
	@Override
	public String toString() {
		return getSQL(SchemaContext.threadContext.get());
	}
	
	// is it an insert, update, delete, select, etc.
	public boolean isDML() { return true; }
	// is it a create statement, either dve metadata or regular metadata
	public boolean isDDL() { return false; }
	// is it a session statement, such as use database
	public boolean isSession() { return false; }
	
	public abstract void normalize(SchemaContext sc);
	
	@SuppressWarnings("unchecked")
	public static PlanningResult prepare(SchemaContext sc, Statement s, BehaviorConfiguration config, String pstmtID, String origSQL) throws PEException {
		if (!(s instanceof CacheableStatement)) {
			throw new PEException("Invalid statement for prepare - " + s.getClass().getSimpleName());
		} 
		s.clearWarnings(sc);
		GenericSQLCommand logFormat = null; 
		ProjectionInfo projection = null;
		if (s.isDML()) {
			DMLStatement dmls = (DMLStatement) s;
            logFormat =	dmls.getGenericSQL(sc, Singletons.require(HostService.class).getDBNative().getEmitter(), null);
			projection = dmls.getProjectionMetadata(sc);
		} else {
			logFormat = new GenericSQLCommand(s.getSQL(sc));
		}
		ExecutionPlan currentPlan = new ExecutionPlan(projection,sc.getValueManager(),StatementType.PREPARE);
		if (s.filterStatement(sc))
			return new PlanningResult(Collections.singletonList(buildFilteredPlan(currentPlan)), null,origSQL);
		// we will build two execution plans here - the first is the one we're going to push down for the prepare
		// and the second is the one for the actual stmt
		List<TableKey> tableKeys = null;
		PEStorageGroup pesg = s.getSingleGroup(sc);
		if (pesg == null)
			pesg = buildOneSiteGroup(sc);
		if (s.isDML()) {
			DMLStatement dmls = (DMLStatement) s;
            currentPlan.getSequence().append(new PrepareExecutionStep(dmls.getDatabase(sc), pesg,
					dmls.getGenericSQL(sc, Singletons.require(HostService.class).getDBNative().getEmitter(), null)));
			tableKeys = dmls.getDerivedInfo().getAllTableKeys();
		} else {
			currentPlan.getSequence().append(new PrepareExecutionStep(s.getDatabase(sc),pesg,new GenericSQLCommand(s.getSQL(sc))));
			tableKeys = Collections.EMPTY_LIST;
		}
		Database<?> cdb = sc.getCurrentDatabase(false);
		if (cdb == null)
			cdb = s.getDatabase(sc);
		PlanCacheKey pck = new PlanCacheKey(origSQL, cdb);
		// look up the execution plan in the plan cache; if it doesn't exist then we'll go ahead and plan, otherwise not so much.
		CachedPreparedStatement pstmtPlan = sc.getSource().getPreparedStatement(pck);
		if (pstmtPlan == null) {
			pstmtPlan = new CachedPreparedStatement(pck, getExecutionPlan(sc, s, config, origSQL), tableKeys, logFormat);
		}
		return new PreparePlanningResult(currentPlan, pstmtPlan, origSQL);		
	}
	
	protected static ExecutionPlan buildFilteredPlan(ExecutionPlan ep) {
		ep.getSequence().append(new EmptyExecutionStep(0,"filtered statement")); 
		ep.setCacheable(true);
		ep.setIsEmptyPlan(true);
		return ep;
	}
	
	public static ExecutionPlan getExecutionPlan(SchemaContext sc, Statement s) throws PEException {
		return getExecutionPlan(sc, s, sc.getBehaviorConfiguration());
	}
	
	public static ExecutionPlan getExecutionPlan(SchemaContext sc, Statement s, BehaviorConfiguration config) throws PEException {
		return getExecutionPlan(sc, s,config,null);
	}
	
	public static ExecutionPlan getExecutionPlan(SchemaContext sc, Statement s, BehaviorConfiguration config, String origSQL) throws PEException {
		ProjectionInfo projection = s.getProjectionMetadata(sc);
		ExecutionPlan ep = new ExecutionPlan(projection,sc.getValueManager(), s.getStatementType());

		s.clearWarnings(sc);
		
		if (s.filterStatement(sc)) 
			return buildFilteredPlan(ep);

		Statement ps = PrePlanner.transform(sc,s);
		if (ps.isExplain()) {
			ExecutionPlan expep = ps.buildExplain(sc, config);
			ep.getSequence().append(expep.generateExplain(sc,ps,origSQL));
		} else {
			ps.planStmt(sc, ep.getSequence(), config, false);
		}
		return ep;
	}
	
	protected ExecutionPlan buildExplain(SchemaContext sc, BehaviorConfiguration config) throws PEException {
		ExecutionPlan expep = new ExecutionPlan(null,sc.getValueManager(), StatementType.EXPLAIN);
		planStmt(sc, expep.getSequence(),config, true);
		return expep;
	}
	
	protected void preplan(SchemaContext sc, ExecutionSequence es, boolean explain) throws PEException {
	}
	
	// made this final so that we always run preplan
	protected final void planStmt(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config, boolean explain) throws PEException {
		preplan(sc, es,explain);
		plan(sc, es, config);
	}

	// warnings support
	protected void clearWarnings(SchemaContext sc) {
		sc.getConnection().getMessageManager().clear();
	}
	
	public ProjectionInfo getProjectionMetadata(SchemaContext sc) {
		return null;
	}
	
	public abstract void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException;

	public StatementType getStatementType() {
		return StatementType.UNIMPORTANT;
	}
	
	public final LockType getLockType() {
		return StatementTraits.getLockType(this.getClass());
	}
	
	// statements can override this if they want
	public Database<?> getDatabase(SchemaContext sc) {
		return sc.getCurrentDatabase();
	}

	public PEStorageGroup getStorageGroup(SchemaContext sc) throws PEException {
		return getSingleGroup(sc);
	}
	
	public PEStorageGroup getSingleGroup(SchemaContext sc) throws PEException {
		List<PEStorageGroup> groups = getStorageGroups(sc);
		if (groups.size() > 1) {
			throw new PEException("More than one persistent group present, more planning needed");
		}
		if (groups.size() == 0)
			throw new PEException("No persistent group present, invalid planning");
		return groups.get(0);		
	}
		
	public List<PEStorageGroup> getStorageGroups(SchemaContext sc) throws PEException {
		return Collections.singletonList((PEStorageGroup)sc.getPersistentGroup());		
	}
	
	protected void unhandledStatement() throws PEException {
		throw new PEException("Unknown statement kind for planning: " + getClass().getName());
	}
	
	protected void unsupportedStatement() throws PEException {
		throw new PEException("Unsupported statement kind for planning: " + getClass().getName());
	}

	@Override
	public <T extends Edge<?,?>> List<T> getEdges() {
		return Collections.emptyList();
	}

	protected final boolean illegalSchemaSelf(LanguageNode other) {
		throw new MigrationException("Illegal call to " + this.getClass().getSimpleName() + ".schemaSelfEqual");		
	}
	
	protected final int illegalSchemaHash() {
		throw new MigrationException("Illegal call to " + this.getClass().getSimpleName() + ".selfHashCode");		
	}
	
	// for any unprintable stuff in the statement, convert literals to '?'.  this is a destructive operation.
	// values of those literals are returned
	public List<Object> extractParameters(SchemaContext sc, Charset currentCharset, Charset desiredCharset) throws PEException {
		if (!(this instanceof DMLStatement)) throw new PEException("Unsupported statement for parameters: not a dml statement");
		ParameterExtractionTraversal pet = new ParameterExtractionTraversal(sc,(DMLStatement)this,currentCharset,desiredCharset);
		pet.traverse(this);
		return pet.getGeneratedParameters();
	}
	
	protected static class ParameterExtractionTraversal extends Traversal {

		protected DMLStatement stmt;
		protected Charset src;
		protected Charset targ;
		
		protected List<Object> generatedParameters;
		
		protected final SchemaContext pc;
		
		public ParameterExtractionTraversal(SchemaContext sc, DMLStatement dmls, Charset srcCharset, Charset targCharset) {
			super(Order.NATURAL_ORDER, ExecStyle.ONCE);
			stmt = dmls;
			pc = sc;
			src = srcCharset;
			targ = targCharset;
			generatedParameters = new ArrayList<Object>();
		}

		public List<Object> getGeneratedParameters() {
			return generatedParameters;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (in instanceof LiteralExpression) {
				LiteralExpression le = (LiteralExpression) in;
				if (le.isNullLiteral())
					return in;
				if (le.isStringLiteral()) {
					// le is a string - we're going to flip it back into bytes and try it against the target character set
					// if that fails, then we replace it, otherwise not so much
                    byte[] bytes =	Singletons.require(HostService.class).getDBNative().getValueConverter().convertBinaryLiteral(le.getValue(pc));
					// String raw = (String) le.getValue();
					String maybe = PECharsetUtils.getString(bytes, targ, true);
					if (maybe != null)
						// it's ok in this charset
						return in;
					Object v = bytes;
					Edge<?, ExpressionNode> pedge = le.getParentEdge();
					Parameter ret = new Parameter(null);
					pedge.set(ret);
					generatedParameters.add(v);
					return ret;
				}
			}
			return in;
		}
	}	

	public PEPersistentGroup buildAllNonUniqueSitesGroup(SchemaContext pc) {
		return buildSiteGroup(pc, false, null, false);
	}
	
	public PEPersistentGroup buildAllSitesGroup(SchemaContext pc) {
		// unroll to create one ddl execution step for each user
		// collect all the persistent sites into an uber group
		return buildAllSitesGroup(pc, null);
	}
	
	public PEPersistentGroup buildAllSitesGroup(SchemaContext pc, Boolean overrideRequiresPrivilegeValue) {
		return buildSiteGroup(pc, false, overrideRequiresPrivilegeValue, true);
	}
	
	public static PEPersistentGroup buildOneSiteGroup(SchemaContext pc) {
		// unroll to create one ddl execution step for each user
		// collect one persistent site (the first one) into a group
		return buildOneSiteGroup(pc, null);
	}
	
	public static PEPersistentGroup buildOneSiteGroup(SchemaContext pc, Boolean overrideRequiresPrivilegeValue) {
		return buildSiteGroup(pc, true, overrideRequiresPrivilegeValue, true);
	}
	
	public static PEPersistentGroup buildSiteGroup(SchemaContext pc, boolean useOneSiteGroup, Boolean overrideRequiresPrivilegeValue, boolean uniquify) {
        ShowInformationSchemaTable sitesTable = Singletons.require(HostService.class).getInformationSchema().lookupShowTable(new UnqualifiedName("persistent site"));
		try {
			pc.getCatalog().startTxn();
			List<CatalogEntity> sites = sitesTable.getLikeSelectEntities(pc, null, null, null, overrideRequiresPrivilegeValue);
			// create a temp group
			final HashSet<String> uniqueURLS = new HashSet<String>();
			List<PersistentSite> persSites = Functional.apply(
					(useOneSiteGroup ? Collections.singletonList(sites.get(0)) : sites), new Cast<PersistentSite, CatalogEntity>());

			List<PersistentSite> returnSites = persSites;
			if (uniquify) {
				returnSites = Functional.select(persSites, new UnaryPredicate<PersistentSite>() {

					@Override
					public boolean test(PersistentSite object) {
						return uniqueURLS.add(object.getMasterUrl());
					}

				});
			}
			return PEPersistentGroup.load(new PersistentGroup(returnSites), pc, true);
			
		} finally {
			pc.getCatalog().commitTxn();
		}
	}

	public boolean filterStatement(SchemaContext sc) { return false; }
}
