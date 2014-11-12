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
import java.util.Locale;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.MTTableKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.AutoIncrementLiteralExpression;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.schema.cache.ConstantType;
import com.tesora.dve.sql.schema.cache.IAutoIncrementLiteralExpression;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;
import com.tesora.dve.sql.schema.cache.IParameter;
import com.tesora.dve.sql.schema.mt.PETenant;
import com.tesora.dve.sql.schema.mt.TableScope;
import com.tesora.dve.variable.VariableConstants;
import com.tesora.dve.variables.KnownVariables;

public class AutoIncrementBlock {
	
	private final TableKey table;
	private TableScope scope = null;
	// maintain the original order of specified and generated values - this allows us to do late allocation even in the
	// case where specified values mean for the current stmt that a value must be generated.
	private List<IConstantExpression> inorder = new ArrayList<IConstantExpression>();
	private List<IAutoIncrementLiteralExpression> generated = new ArrayList<IAutoIncrementLiteralExpression>();
	private final ValueManager containing;
	
	public AutoIncrementBlock(ValueManager vm, TableKey tk) {
		table = tk.makeFrozen();
		if (tk instanceof MTTableKey)
			scope = ((MTTableKey)tk).getScope();
		containing = vm;
	}

	private AutoIncrementBlock(AutoIncrementBlock other) {
		table = other.table;
		scope = other.scope;
		containing = other.containing;	
		// generated is only used during planning - once we're frozen we don't need it
		generated = null;
		inorder = new ArrayList<IConstantExpression>();
		for(IConstantExpression idle : other.inorder) 
			inorder.add(idle.getCacheExpression());
	}
		
	public AutoIncrementBlock makeCopy() {
		return new AutoIncrementBlock(this);
	}
	
	public AutoIncrementLiteralExpression allocateAutoIncrementExpression(ConnectionValues cv) {
		AutoIncrementLiteralExpression aile = new AutoIncrementLiteralExpression(cv,generated.size());
		generated.add(aile);
		inorder.add(aile);
		return aile;
	}
	
	public void registerSpecifiedAutoInc(ConstantExpression dle) {
		inorder.add(dle);
	}

	public void compute(SchemaContext sc, ConnectionValues cv) {
		SQLMode mode = 
				KnownVariables.SQL_MODE.getSessionValue(cv.getConnection().getVariableSource());
		cv.setLastInsertId(null);
		// examine the values in order to determine whether insert id is illegal
		long max = -1;
		List<IConstantExpression> toAllocate= new ArrayList<IConstantExpression>();
		for(IConstantExpression dle : inorder) {
			if (dle instanceof IAutoIncrementLiteralExpression) {
				toAllocate.add(dle);
			} else {
				if (requiresAllocation(dle,cv,mode)) {
					toAllocate.add(dle);
				} else {
					Long value = null;
					Object object = dle.getValue(cv);
					if (object instanceof String) {
						value = Long.valueOf((String)object);
					} else {
						value = Long.valueOf(((Number)object).longValue());
					}
					if (value.longValue() > max) max = value.longValue();
				}
			}
		}
		Long insertIdFromVar = 
				KnownVariables.REPL_INSERT_ID.getValue(cv.getConnection().getVariableSource());
		
		if (max > -1 && insertIdFromVar != null)
			throw new SchemaException(Pass.SECOND, "Cannot specify both the autoincrement column value and " + VariableConstants.REPL_SLAVE_INSERT_ID_NAME);
		TableScope actualScope = null;
		TableKey tk = null;
		if (scope != null) {
			PETenant currentTenant = (PETenant) sc.getCurrentTenant().get(sc);
			actualScope = currentTenant.lookupScope(sc, scope.getName(), null /* should have locked already */);
			tk = TableKey.make(sc,actualScope,0);
		} else {
			tk = table;
		}
		if (!toAllocate.isEmpty()) {
			long first = -1;
			if (insertIdFromVar == null) {
				first = tk.getNextAutoIncrBlock(sc, toAllocate.size());
			} else {
				first = insertIdFromVar.longValue();
			}
			int i = 0;
			for(IConstantExpression dle : toAllocate) {
				Long aiv = new Long(first + i);
				if (dle instanceof IAutoIncrementLiteralExpression) {
					cv.resetAutoIncValue(dle.getPosition(), aiv);	
				} else if (dle.getConstantType() == ConstantType.PARAMETER) {
					cv.resetParameterValue(dle.getPosition(), aiv);
				} else {
					cv.setLiteralValue(dle.getPosition(), aiv);
				}
				i++;
			}
			if (insertIdFromVar != null) {
				max = first + (toAllocate.size() - 1);
			}
			cv.setLastInsertId(first + (toAllocate.size() - 1));
		}
		if (max > -1) 
			tk.removeValue(sc, max);
	}
	
	public static boolean requiresAllocation(IConstantExpression in, ConnectionValues cv, SQLMode mode) {
		if (in instanceof ILiteralExpression) {
			ILiteralExpression ile = (ILiteralExpression) in;
			if (ile.isNullLiteral())
				return true;
			Object rv = ile.getValue(cv);
			return requiresAutoIncAllocation(rv,!ile.isStringLiteral(),mode);
		} else if (in.getConstantType() == ConstantType.PARAMETER) { 
			IParameter ip = (IParameter) in;
			Object v = ip.getValue(cv);
			return requiresAutoIncAllocation(v,true,mode);			
		} 
		return false;
	}
	
	private static boolean requiresAutoIncAllocation(Object v, boolean nullok, SQLMode mode) {
		if (v instanceof String) {
			String sv = (String) v;
			if ("NULL".equals(sv.toUpperCase(Locale.ENGLISH)))
				return true;
			Long value = null;
			try {
				value = Long.valueOf(sv);
			} catch (Throwable t) {
				value = null;
			}
			if (value == null) return false;
			return (value.longValue() == 0 && !mode.isNoAutoOnZero());
		} else if (v instanceof Number) {
			Number n = (Number) v;
			return (n.longValue() == 0 && !mode.isNoAutoOnZero());
		} else if (v == null) {
			if (nullok)
				return true;
			throw new SchemaException(Pass.PLANNER, "Invalid specified value for autoincrement column - null string");				
		}
		return false;
	}
	
}