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
import java.util.EnumMap;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.AutoIncrementLiteralExpression;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.node.expression.ValueSource;
import com.tesora.dve.sql.parser.TimestampVariableUtils;
import com.tesora.dve.sql.schema.cache.ConstantType;
import com.tesora.dve.sql.schema.cache.IAutoIncrementLiteralExpression;
import com.tesora.dve.sql.schema.cache.IDelegatingLiteralExpression;
import com.tesora.dve.sql.schema.cache.IParameter;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.util.ResizableArray;

public class ConnectionValues implements ValueSource {

	private EnumMap<ConstantType,ResizableArray<Object>> constantValues = new EnumMap<ConstantType,ResizableArray<Object>>(ConstantType.class);
	
	private AutoIncrementBlock autoincs = null;

	private Long tenantID = null;
	// set if this tenant id represents a container tenant.
	private SchemaCacheKey<PEContainer> containerTenant = null;

	private List<UnqualifiedName> tempTables = new ArrayList<UnqualifiedName>();
	
	private List<PEStorageGroup> placeholderGroups = new ArrayList<PEStorageGroup>();

	private ResizableArray<Long> autoincValues = new ResizableArray<Long>();
	
	private Long lastInsertId;
	
	private List<JustInTimeInsert> lateSortedInsert = null;
	
	private final boolean original;
	
	private final ValueManager types;
	private final ConnectionContext connection;
	
	private long currentTimestamp = 0;

	public ConnectionValues(ValueManager vm, ConnectionContext cc) {
		original = true;
		types = vm;
		connection = cc;
		for(ConstantType ct : ConstantType.values()) {
			if (ct == ConstantType.PARAMETER)
				constantValues.put(ct,  null);
			else
				constantValues.put(ct, new ResizableArray<Object>());
		}
	}
	
	private ConnectionValues(ConnectionValues other, ConnectionContext cc) {
		connection = cc;
		types = other.types;
		for(ConstantType ct : ConstantType.values()) {
			ResizableArray<Object> ovals = other.constantValues.get(ct);
			if (ct == ConstantType.PARAMETER) {
				constantValues.put(ct, ovals);
			} else {
				constantValues.put(ct,new ResizableArray<Object>(ovals == null ? 0 : ovals.size()));
			}
		}
		tenantID = other.tenantID;
		tempTables = new ArrayList<UnqualifiedName>(other.tempTables);
		autoincs = (other.autoincs == null ? null : other.autoincs.makeCopy());
		autoincValues = new ResizableArray<Long>(other.autoincValues.size());
		placeholderGroups = new ArrayList<PEStorageGroup>(other.placeholderGroups);
		original = false;
		lastInsertId = null;
		lateSortedInsert = null;
	}
	
	public ConnectionValues makeCopy(ConnectionContext cc) {
		return new ConnectionValues(this,cc);
	}
	
	public boolean isOriginal() {
		return original;
	}
	
	public int allocateTempTableName(String tempTableName) {
		UnqualifiedName nn = new UnqualifiedName(tempTableName);
		int index = tempTables.size();
		tempTables.add(nn);
		return index;
	}

	public UnqualifiedName getTempTableName(int index) {
		return tempTables.get(index);
	}
	
	public void resetTempTables(SchemaContext sc) {
		if (tempTables.isEmpty()) return;
		int ntemp = tempTables.size();
		tempTables.clear();
		for(int i = 0; i < ntemp; i++)
			tempTables.add(new UnqualifiedName(sc.getCatalog().getTempTableName()));
	}
	
	public void resetTempGroups() throws PEException {
		ArrayList<PEStorageGroup> ng = new ArrayList<PEStorageGroup>();
		for(PEStorageGroup pg : placeholderGroups) {
			if (pg.isTempGroup()) {
				PEDynamicGroup pedg = (PEDynamicGroup) pg;
				ng.add(pedg.copy());
			} else {
				ng.add(pg);
			}
		}
		placeholderGroups = ng;
	}
	
	public void resetTenantID(Long id, SchemaCacheKey<PEContainer> onContainer) throws PEException {
		tenantID = id; 
		if (containerTenant != null && onContainer == null)
			throw new PEException("Expect a container tenant, not a schema tenant");
		else if (containerTenant == null && onContainer != null)
			throw new PEException("Expect a schema tenant, not a container tenant");
		else if (containerTenant != null && onContainer != null && !containerTenant.equals(onContainer))
			throw new PEException("Wrong container tenant");
	}

	public Object getLiteralValue(int index) {
		return constantValues.get(ConstantType.LITERAL).get(index);
	}
	
	public void setLiteralValue(int index, Object value) {
		constantValues.get(ConstantType.LITERAL).set(index, value);
	}
	
	public int getNumberOfLiterals() {
		return constantValues.get(ConstantType.LITERAL).size();
	}

	public void setParameters(List<Object> params) {
		constantValues.put(ConstantType.PARAMETER, new ResizableArray<Object>(params));
	}
	
	public Object getParameterValue(int index) {
		ResizableArray<Object> vals = constantValues.get(ConstantType.PARAMETER);
		if (vals == null) return null;
		return vals.get(index);
	}
	
	public void resetParameterValue(int index, Object newVal) {
		ResizableArray<Object> vals = constantValues.get(ConstantType.PARAMETER);
		if (vals == null) return;
		vals.set(index, newVal);
	}
	
	public void setRuntimeConstants(List<Object> vals) {
		constantValues.put(ConstantType.RUNTIME, new ResizableArray<Object>(vals));
	}
	
	public Object getRuntimeConstant(int index) {
		return constantValues.get(ConstantType.RUNTIME).get(index);
	}
	
	
	public Long getTenantID() {
		return tenantID;
	}

	public void setTenantID(Long v, SchemaCacheKey<PEContainer> expectedContainer) {
		tenantID = v;
		containerTenant = expectedContainer;
	}
	
	public void allocateAutoIncBlock(TableKey tk) {
		if (autoincs != null) 
			throw new SchemaException(Pass.SECOND, "Duplicate autoinc block");
		autoincs = new AutoIncrementBlock(types,tk);
	}

	public AutoIncrementLiteralExpression allocateAutoInc() {
		if (autoincs == null) throw new SchemaException(Pass.SECOND, "Missing autoinc block");
		return autoincs.allocateAutoIncrementExpression(this);
	}
	
	public void registerSpecifiedAutoinc(ConstantExpression dle) {
		if (autoincs == null) throw new SchemaException(Pass.SECOND, "Missing autoinc block");
		autoincs.registerSpecifiedAutoInc(dle);
	}
	
	public void handleAutoincrementValues(SchemaContext sc) {
		if (autoincs == null) return;
		autoincs.compute(sc, this);
	}

	public Object getAutoincValue(IAutoIncrementLiteralExpression expr) {
		if (autoincs == null) throw new SchemaException(Pass.SECOND, "Missing autoinc block");
		return autoincValues.get(expr.getPosition());
	}
	
	public void resetAutoIncValue(int position, Long value) {
		autoincValues.set(position, value);
	}
	
	public Long getLastInsertId() {
		return lastInsertId;
	}
		
	public void setLastInsertId(Long v) {
		lastInsertId = v;
	}
	
	public int allocatePlaceholderGroup(PEStorageGroup pesg) {
		int res = placeholderGroups.size();
		placeholderGroups.add(pesg);
		return res;
	}
	
	public PEStorageGroup getPlaceholderGroup(int index) {
		return placeholderGroups.get(index);
	}

	public long getCurrentTimestamp() {
		if (currentTimestamp == 0) {
			resetCurrentTimestamp();
		}
		return currentTimestamp;
	}
	
	public void setLateSortedInsert(List<JustInTimeInsert> stmts) {
		lateSortedInsert = stmts;
	}
	
	public List<JustInTimeInsert> getLateSortedInserts() {
		return lateSortedInsert;
	}
	
	public void clearCurrentTimestamp() {
		this.currentTimestamp = 0;
	}

	private void resetCurrentTimestamp() {
		this.currentTimestamp = TimestampVariableUtils.getCurrentUnixTime(connection);
	}

	@Override
	public Object getValue(IParameter p) {
		return getParameterValue(p.getPosition());
	}

	@Override
	public Object getLiteral(IDelegatingLiteralExpression dle) {
		try {
			Type any = types.getLiteralType(dle);
			Object value = getLiteralValue(dle.getPosition());
			if (any != null && value != null)
                return Singletons.require(HostService.class).getDBNative().getValueConverter().convert(value, any);
			return value;
		} catch (Throwable t) {
			throw new SchemaException(Pass.PLANNER, "Literal for index " + dle.getPosition() + " is invalid",t);
		}
	}
	
	public ConnectionContext getConnection() {
		return connection;
	}
}
