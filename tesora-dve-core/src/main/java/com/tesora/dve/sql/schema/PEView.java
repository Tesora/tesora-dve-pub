package com.tesora.dve.sql.schema;

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

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.UserView;
import com.tesora.dve.common.catalog.ViewMode;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ExpressionPath;
import com.tesora.dve.sql.expression.SetQuantifier;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.transform.AggFunCollector;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.util.ListSet;

public class PEView extends Persistable<PEView,UserView> {

	// definer 
	private SchemaEdge<PEUser> definer;
	// the src query
	// when creating this is the actual query;
	// when loading this is the stmt using the global cache objects
	private ProjectingStatement viewDefinition;
	private String rawSQL;
	// charset and collation at time of definition
	private UnqualifiedName charset;
	private UnqualifiedName collation;
	private ViewMode mode;
	private String check;
	private String algorithm;
	private String security;
	
	// this a runtime only value - indicates whether we can try to merge this view
	// this is set when viewDefinition is set - but only for nonmutable sources
	private Boolean merge;
	
	public PEView(SchemaContext sc, 
			UnqualifiedName name, PEDatabase ofDB, 
			PEUser definer, 
			ProjectingStatement definingQuery,
			UnqualifiedName charset,
			UnqualifiedName collation,
			ViewMode m,
			String checkMode,
			String algorithm,
			String security) {
		super(getCacheKey(ofDB.getName(),name));
		viewDefinition = definingQuery;
		rawSQL = viewDefinition.getSQL(sc);
		setUser(sc,definer,false);
		setName(name);
		this.charset = charset;
		this.collation = collation;
		this.mode = m;
		this.check = checkMode;
		this.algorithm = algorithm;
		this.security = security;
		setPersistent(sc,null,null);
	}
	
	// used in view table recreate
	public PEView(SchemaContext sc, PEView pv, PEViewTable viewTab) {
		super(pv.getCacheKey());
		viewDefinition = CopyVisitor.copy(pv.getViewDefinition(sc, viewTab, false));
		rawSQL = pv.rawSQL;
		setUser(sc,sc.getSource().find(sc, pv.definer.getCacheKey()),false);
		setName(pv.getName());
		this.charset = pv.charset;
		this.collation = pv.collation;
		this.mode = pv.mode;
		this.check = pv.check;
		this.mode = pv.mode;
		this.algorithm = pv.algorithm;
		this.security = pv.security;
		setPersistent(sc,null,null);
	}
	
	protected PEView(UserView uv, SchemaContext sc) {
		super(getCacheKey(uv.getTable().getDatabase().getName(),uv.getTable().getName()));
		setName(new UnqualifiedName(uv.getTable().getName()));
		setUser(sc,PEUser.load(uv.getDefiner(), sc),true);
		viewDefinition = null;
		rawSQL = uv.getDefinition();
		this.charset = new UnqualifiedName(uv.getCharset());
		this.collation = new UnqualifiedName(uv.getCollation());
		this.mode = uv.getViewMode();
		this.check = uv.getCheckOption();
		this.algorithm = uv.getAlgorithm();
		this.security = uv.getSecurity();
		setPersistent(sc,uv,uv.getId());
	}
	
	public static PEView load(UserView uv, SchemaContext sc) {
		PEView pep = (PEView) sc.getLoaded(uv,getCacheKey(uv.getTable().getDatabase().getName(),uv.getTable().getName()));
		if (pep == null)
			pep = new PEView(uv,sc);
		return pep;
	}

	
	public PEUser getDefiner(SchemaContext sc) {
		return definer.get(sc);
	}

	@SuppressWarnings("unchecked")
	public void setUser(SchemaContext sc, PEUser user, boolean persistent) {
		this.definer = StructuralUtils.buildEdge(sc, user, persistent);
	}
	
	public ProjectingStatement getViewDefinition(SchemaContext sc, PEViewTable viewTab, boolean lockTables) {
		ProjectingStatement out = null;
		if (sc.isMutableSource() || viewDefinition == null) {
			out = (ProjectingStatement)buildStatement(sc,
					(viewTab == null ? null : viewTab.getPEDatabase(sc)),rawSQL, lockTables);
			if (merge == null)
				merge = computeMergeFlag(out);
		}
		if (sc.isMutableSource()) return out;
		if (viewDefinition == null)
			viewDefinition = out;
		return viewDefinition;
	}

	private Statement buildStatement(SchemaContext context, PEDatabase ondb, String raw, boolean locking) {
		SchemaContext sc = context;
		if (!sc.isMutableSource()) 
			sc = SchemaContext.makeImmutableIndependentContext(context);
		PEDatabase cdb = sc.getCurrentPEDatabase(false);
		if (ondb != null)
			sc.setCurrentDatabase(ondb);
		// reparse to get the right schema objects, and force all literals to be actual literals
		ParserOptions originalOptions = sc.getOptions();

		ParserOptions myOpts = originalOptions;
		if (myOpts == null)
			myOpts = context.getOptions();
		if (myOpts == null)
			myOpts = ParserOptions.NONE;
		myOpts = myOpts.setActualLiterals().setResolve();
		if (!locking)
			myOpts = myOpts.setIgnoreLocking();
		Statement out = null;
		try {
			out = InvokeParser.parse(raw, sc, Collections.emptyList(),myOpts).get(0);
		} finally {
			sc.setOptions(originalOptions);
			sc.setCurrentDatabase(cdb);
		}
		return out;
	}
	
	public Boolean isMerge(SchemaContext sc, PEViewTable viewTab) {
		if (merge == null)
			getViewDefinition(sc, viewTab, false);
		return merge;
	}
	
	public UnqualifiedName getCharset() {
		return charset;
	}
	
	public UnqualifiedName getCollation() {
		return collation;
	}
	
	public ViewMode getMode() {
		return mode;
	}
	
	public String getAlgorithm() {
		return algorithm;
	}
	
	public void setAlgorithm(String v) {
		algorithm = v;
	}
	
	public String getCheckOption() {
		return check;
	}
	
	public String getSecurity() {
		return security;
	}

	public void setSecurity(String s) {
		security = s;
	}
	
	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return UserView.class;
	}

	@Override
	protected String getDiffTag() {
		return "view";
	}

	@Override
	protected int getID(UserView p) {
		return p.getId();
	}

	@Override
	protected UserView lookup(SchemaContext sc) throws PEException {
		return null;
	}

	@Override
	protected UserView createEmptyNew(SchemaContext sc) throws PEException {
		return new UserView(definer.get(sc).persistTree(sc),viewDefinition.getSQL(sc),
				charset.get(),collation.get(),
				mode,
				security, algorithm, check);
	}

	@Override
	protected void populateNew(SchemaContext sc, UserView p) throws PEException {
	}

	@Override
	protected Persistable<PEView, UserView> load(SchemaContext sc, UserView p)
			throws PEException {
		return null;
	}

	public static SchemaCacheKey<?> getCacheKey(Name dbName, Name viewName) {
		return getCacheKey(dbName.getUnqualified().getUnquotedName().get(), viewName.getUnqualified().getUnquotedName().get());
	}
	
	public static SchemaCacheKey<?> getCacheKey(String dbName, String viewName) {
		return new ViewCacheKey(dbName, viewName);
	}
	
	public static class ViewCacheKey extends SchemaCacheKey<PEView> {

		private static final long serialVersionUID = 1L;

		private String dbName;
		private String viewName;

		public ViewCacheKey(String dbn, String vn) {
			super();
			this.dbName = dbn;
			this.viewName = vn;
		}
		
		@Override
		public int hashCode() {
			return addHash(initHash(PEView.class, viewName.hashCode()),dbName.hashCode());
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof ViewCacheKey) {
				ViewCacheKey vck = (ViewCacheKey) o;
				return dbName.equals(vck.dbName) && viewName.equals(vck.viewName);
			}
			return false;
		}

		@Override
		public PEView load(SchemaContext sc) {
			UserView uv = sc.getCatalog().findView(viewName, dbName);
			if (uv == null)
				return null;
			return PEView.load(uv, sc);
		}

		@Override
		public String toString() {
			return "PEView:" + dbName + "." + viewName;
		}
		
	}

	
	private boolean computeMergeFlag(ProjectingStatement ps) {
		if ("TEMPTABLE".equals(algorithm)) return false;
		if (ps.getLimit() != null)
			return false;
		if (ps instanceof UnionStatement)
			return false;
		SelectStatement ss = (SelectStatement) ps;
		if (ss.getSetQuantifier() == SetQuantifier.DISTINCT)
			return false;
		if (ss.getHavingEdge().has())
			return false;
		if (ss.getGroupBysEdge().has())
			return false;
		for(ProjectingStatement ips : ss.getDerivedInfo().getLocalNestedQueries()) {
			if (ips.getParentEdge().getName().matches(EdgeName.SUBQUERY)) {
				ExpressionPath ep = ExpressionPath.build(ips, ss);
				if (ep.has(EngineConstant.PROJECTION))
					return false;
			}
		}
		ListSet<FunctionCall> any = AggFunCollector.collectAggFuns(ss.getProjectionEdge());
		return any.isEmpty();
	}
	
}
