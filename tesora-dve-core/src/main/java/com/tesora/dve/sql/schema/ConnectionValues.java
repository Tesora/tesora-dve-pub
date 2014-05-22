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
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.AutoIncrementLiteralExpression;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.schema.cache.IAutoIncrementLiteralExpression;
import com.tesora.dve.sql.util.ResizableArray;
import com.tesora.dve.sql.parser.TimestampVariableUtils;

public class ConnectionValues {

	private ResizableArray<Object> literalValues = new ResizableArray<Object>();
	
	private AutoIncrementBlock autoincs = null;

	private Long tenantID = null;

	private List<UnqualifiedName> tempTables = new ArrayList<UnqualifiedName>();
	
	private List<PEStorageGroup> placeholderGroups = new ArrayList<PEStorageGroup>();

	private ResizableArray<Long> autoincValues = new ResizableArray<Long>();
	
	private List<Object> parameterValues = null;
	
	private Long lastInsertId;
	
	private List<JustInTimeInsert> lateSortedInsert = null;
	
	private final boolean original;
	
	private long currentTimestamp = 0;

	public ConnectionValues() {
		original = true;
	}
	
	private ConnectionValues(ConnectionValues other) {
		literalValues = new ResizableArray<Object>(other.literalValues.size()); 
		tenantID = other.tenantID;
		tempTables = new ArrayList<UnqualifiedName>(other.tempTables);
		autoincs = (other.autoincs == null ? null : other.autoincs.makeCopy());
		autoincValues = new ResizableArray<Long>(other.autoincValues.size());
		placeholderGroups = new ArrayList<PEStorageGroup>(other.placeholderGroups);
		parameterValues = (other.parameterValues == null ? null : other.parameterValues);
		original = false;
		lastInsertId = null;
		lateSortedInsert = null;
	}
	
	public ConnectionValues makeCopy() {
		return new ConnectionValues(this);
	}
	
	public boolean isOriginal() {
		return original;
	}
	
	public int allocateTempTableName(SchemaContext sc) {
		UnqualifiedName nn = new UnqualifiedName(sc.getCatalog().getTempTableName());
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
	
	public void resetTempGroups(SchemaContext sc) throws PEException {
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
	
	public void resetTenantID(SchemaContext sc) throws PEException {
		tenantID = sc.getPolicyContext().getTenantID(false);
	}
	
	public Object getLiteralValue(int index) {
		return literalValues.get(index);
	}
	
	public void setLiteralValue(int index, Object value) {
		literalValues.set(index, value);
	}
	
	public int getNumberOfLiterals() {
		return literalValues.size();
	}

	public void setParameters(List<?> params) {
		parameterValues = new ArrayList<Object>(params);
	}
	
	public Object getParameterValue(int index) {
		if (parameterValues == null)
			return null;
		return parameterValues.get(index);
	}
	
	public void resetParameterValue(int index, Object newVal) {
		if (parameterValues == null) return;
		parameterValues.remove(index);
		parameterValues.add(index, newVal);
	}
	
	public Long getTenantID() {
		return tenantID;
	}

	public void setTenantID(Long v) {
		tenantID = v;
	}
	
	public void allocateAutoIncBlock(ValueManager vm, TableKey tk) {
		if (autoincs != null) throw new SchemaException(Pass.SECOND, "Duplicate autoinc block");
		autoincs = new AutoIncrementBlock(vm,tk);
	}

	public AutoIncrementLiteralExpression allocateAutoInc() {
		if (autoincs == null) throw new SchemaException(Pass.SECOND, "Missing autoinc block");
		return autoincs.allocateAutoIncrementExpression();
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

	public long getCurrentTimestamp(SchemaContext sc) {
		if (currentTimestamp == 0) {
			resetCurrentTimestamp(sc);
		}
		return currentTimestamp;
	}
	
	public void setLateSortedInsert(List<JustInTimeInsert> stmts) {
		lateSortedInsert = stmts;
	}
	
	public List<JustInTimeInsert> getLateSortedInserts() {
		return lateSortedInsert;
	}
	
	public void resetCurrentTimestamp(SchemaContext sc) {
		this.currentTimestamp = TimestampVariableUtils.getCurrentUnixTime(sc);
	}
}
