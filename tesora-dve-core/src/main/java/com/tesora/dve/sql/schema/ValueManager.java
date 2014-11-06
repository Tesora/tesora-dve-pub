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

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.AutoIncrementLiteralExpression;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.node.expression.DelegatingLiteralExpression;
import com.tesora.dve.sql.node.expression.Parameter;
import com.tesora.dve.sql.node.expression.ValueSource;
import com.tesora.dve.sql.parser.ExtractedLiteral;
import com.tesora.dve.sql.schema.cache.IAutoIncrementLiteralExpression;
import com.tesora.dve.sql.schema.cache.IDelegatingLiteralExpression;
import com.tesora.dve.sql.schema.cache.IParameter;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.mt.IPETenant;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.util.ResizableArray;

public class ValueManager {

	// we store the original literal expression so that we can get the type off of them
	// when we reset the literal values we'll use the types in the expressions to figure out
	// how to interpret them
	private ResizableArray<IDelegatingLiteralExpression> literals = new ResizableArray<IDelegatingLiteralExpression>();
	// for inserts, the literal type needs to be used as well - we'll convert it on the way back out if known
	private ResizableArray<Type> literalTypes = new ResizableArray<Type>();
	
	private ResizableArray<IParameter> parameters = new ResizableArray<IParameter>();
	
	private LateSortedInsert lsi = null;
	
	private ConnectionValues values = null;

    public enum CacheStatus { CACHEABLE, NOCACHE_TOO_MANY_LITERALS, NOCACHE_DYNAMIC_FUNCTION}
    private CacheStatus cacheStatus = CacheStatus.CACHEABLE;
	
	private boolean frozen = false;

	// this is set for bad characters - but not during pstmt execution
	private final boolean passDownParameters;
	
	public ValueManager() {
		passDownParameters = false;
	}
	
	public ValueManager(SchemaContext sc, List<Object> nonPrepParams) {
		if (nonPrepParams == null || nonPrepParams.isEmpty())
			passDownParameters = false;
		else {
			passDownParameters = true;
			ConnectionValues cv = getValues(sc,false);
			cv.setParameters(nonPrepParams);
		}
	}
	
	public void setFrozen() {
		if (isCacheable()) {
			// swap out existing objects for cache objects
			for(int i = 0; i < literals.size(); i++) {
				IDelegatingLiteralExpression idle = literals.get(i);
				if (idle == null) continue;
				literals.set(i, (IDelegatingLiteralExpression) idle.getCacheExpression());
			}
			for(int i = 0; i < parameters.size(); i++) {
				IParameter ip = parameters.get(i);
				if (ip == null) continue;
				parameters.set(i, (IParameter)ip.getCacheExpression());
			}
		}
		if (values != null) {
			values = values.makeCopy(null);
		}
		
		frozen = true;
	}
	
	public ConnectionValues getValues(SchemaContext sc) {
		return getValues(sc,true);
	}
	
	public ConnectionValues getValues(SchemaContext sc, boolean check) {
		ConnectionValues cv = sc.getValues();
		if (cv == null) {
			values = new ConnectionValues(this,sc.getConnection());
			cv = values;
			sc.setValues(values);
		} else if (check && cv.getNumberOfLiterals() < literals.size()) {
			throw new SchemaException(Pass.REWRITER, "invalid cached literals.  require " + literals.size() + " but found " + cv.getNumberOfLiterals());
		}
		return cv;
	}
	
	public boolean hasPassDownParams() {
		return passDownParameters;
	}
	
	public Object getValue(SchemaContext sc, IParameter p) {
		return getValues(sc).getParameterValue(p.getPosition());
	}
	
	public int getNumberOfLiterals() {
		return literals.size();
	}

	public List<IDelegatingLiteralExpression> getRawLiterals() {
		ArrayList<IDelegatingLiteralExpression> out = new ArrayList<IDelegatingLiteralExpression>(literals.size());
		for(int i = 0; i < literals.size(); i++) {
			out.add(literals.get(i));
		}
		return out;
	}
	
	public String getOriginalLiteralText(int i) {
		DelegatingLiteralExpression dle = (DelegatingLiteralExpression) literals.get(i);
		return dle.getSourceLocation().getText();
	}
	
	public Object getLiteral(SchemaContext sc,  IDelegatingLiteralExpression dle) {
		return getValues(sc).getLiteral(dle);
	}

	private void checkFrozen(String message) {
		if (frozen) throw new SchemaException(Pass.PLANNER, "Attempt to modify existing cached plan: " + message);
	}
	
	public void markUncacheable(CacheStatus status) {
        cacheStatus = status;
	}
	
	public boolean isCacheable() {
        return cacheStatus == CacheStatus.CACHEABLE;
	}

    public CacheStatus getCacheStatus(){
        return cacheStatus;
    }
	
	public void addLiteralValue(SchemaContext sc, int i, Object v, DelegatingLiteralExpression dle) {
		checkFrozen("add new literal value");
		IDelegatingLiteralExpression already = literals.get(i);
		if (already != null) throw new SchemaException(Pass.SECOND, "Duplicate delegating literal");
		literals.set(i, dle);
		getValues(sc,false).setLiteralValue(i, v);
	}
	
	public void setLiteralType(DelegatingLiteralExpression dle, Type t) {
		checkFrozen("set literal type");
		if (dle instanceof AutoIncrementLiteralExpression) {
		} else {
			literalTypes.set(dle.getPosition(), t);
		}
	}
	
	public Type getLiteralType(IDelegatingLiteralExpression dle) {
		if (dle instanceof IAutoIncrementLiteralExpression)
			return null;
		return literalTypes.get(dle.getPosition());
	}
	
	public void registerParameter(SchemaContext sc, Parameter p) {
		checkFrozen("add a new parameter");
		IParameter already = parameters.get(p.getPosition());
		if (already != null)
			throw new SchemaException(Pass.PLANNER, "Duplicate parameter at position " + p.getPosition());
		parameters.set(p.getPosition(), p);
	}
	
	public int getNumberOfParameters() {
		return parameters.size();
	}
	
	public Object getTenantID(SchemaContext sc) {
		return getValues(sc).getTenantID();
	}
	
	public void setTenantID(SchemaContext sc, Long v, SchemaCacheKey<PEContainer> container) {
		getValues(sc,false).setTenantID(v, container);
	}
	
	public void allocateAutoIncBlock(SchemaContext sc, TableKey tk) {
		checkFrozen("allocate autoincrement values");
		getValues(sc).allocateAutoIncBlock(tk);
	}
	
	public AutoIncrementLiteralExpression allocateAutoInc(SchemaContext sc) {
		checkFrozen("allocate autoincrement value");
		return getValues(sc).allocateAutoInc();
	}
	
	public void registerSpecifiedAutoinc(SchemaContext sc, ConstantExpression dle) {
		checkFrozen("register explicit autoincrement value");
		getValues(sc).registerSpecifiedAutoinc(dle);
	}
	
	public void registerLateSortedInsert(LateSortedInsert lsi) {
		checkFrozen("register late sorted insert");
		if (this.lsi != null)
			throw new SchemaException(Pass.PLANNER, "Already specified a late sorted insert block");
		this.lsi = lsi;
	}
	
	public Object getAutoincValue(SchemaContext sc, IAutoIncrementLiteralExpression exp) {
		return getValues(sc).getAutoincValue(exp);
	}
	
	public Long getLastInsertId(SchemaContext sc) {
		return getValues(sc).getLastInsertId();
	}

	public void resetForNewPStmtExec(SchemaContext sc, List<Object> params) throws PEException {
		ConnectionValues cv = basicReset(sc);
		cv.setParameters(params);		
		cv.handleAutoincrementValues(sc);
		handleLateSortedInsert(sc);
	}
	
	public void resetForNewPlan(SchemaContext sc, List<ExtractedLiteral> literalValues) throws PEException {
		ConnectionValues cv = basicReset(sc);
		for(int i = 0; i < literalValues.size(); i++) {
			IDelegatingLiteralExpression dle = literals.get(i);
            Object intermediate = Singletons.require(HostService.class).getDBNative().getValueConverter().convertLiteral(literalValues.get(i).getText(), dle.getValueType());
			cv.setLiteralValue(i, intermediate);
		}
		cv.handleAutoincrementValues(sc);
		handleLateSortedInsert(sc);
	}	

	private ConnectionValues basicReset(SchemaContext sc) throws PEException {
		SchemaCacheKey<PEContainer> container = null;
		if (sc.getPolicyContext().isContainerContext()) {
			IPETenant ipe = sc.getPolicyContext().getCurrentTenant();
			if (ipe instanceof PEContainerTenant) {
				PEContainerTenant pect = (PEContainerTenant) ipe;
				container = pect.getContainerCacheKey();
			}
		}
		ConnectionValues cv = (values == null ? new ConnectionValues(this,sc.getConnection()) : values.makeCopy(sc.getConnection()));
		sc.setValues(cv);
		sc.setValueManager(this);
		cv.setTenantID(sc.getPolicyContext().getTenantID(false),container);
		cv.resetTempTables(sc);
		cv.resetTempGroups();
		cv.resetTenantID(sc.getPolicyContext().getTenantID(false),container);
		cv.clearCurrentTimestamp();

		return cv;
	}
	
	public void handleAutoincrementValues(SchemaContext sc) {
		getValues(sc,false).handleAutoincrementValues(sc);
	}
	
	public void handleLateSortedInsert(SchemaContext sc) throws PEException {
		if (lsi == null) return;
		List<JustInTimeInsert> computed = lsi.resolve(sc); 
		getValues(sc,false).setLateSortedInsert(computed);
	}
	
	public List<JustInTimeInsert> getLateSortedInsert(SchemaContext sc) {
		return getValues(sc).getLateSortedInserts();
	}
	
	public int allocateTempTableName(SchemaContext sc) {
		checkFrozen("allocate temp table");
		return getValues(sc).allocateTempTableName(sc.getCatalog().getTempTableName());
	}
	
	public int allocatePlaceholderGroup(SchemaContext sc, PEStorageGroup dynGroup) {
		checkFrozen("allocate placeholder persistent group");
		return getValues(sc).allocatePlaceholderGroup(dynGroup);
	}
	
	public PEStorageGroup getPlaceholderGroup(SchemaContext sc, int index) {
		return getValues(sc).getPlaceholderGroup(index);
	}
	
	public long getCurrentTimestamp(SchemaContext sc) {
		return getValues(sc).getCurrentTimestamp();
	}
}
