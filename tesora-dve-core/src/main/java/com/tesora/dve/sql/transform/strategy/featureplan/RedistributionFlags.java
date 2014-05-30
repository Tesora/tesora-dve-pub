package com.tesora.dve.sql.transform.strategy.featureplan;

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

import java.util.List;

import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.PEColumn;

public class RedistributionFlags {

	private PEColumn missingAutoIncrement = null;
	private Integer offsetOfExistingAutoInc = null;
	private List<ExpressionNode> onDuplicateKey = null;
	private Boolean useRowCount = false;
	private boolean mustEnforceScalarValue = false;
	private boolean insertIgnore = false;
	
	public RedistributionFlags() {
	}

	public RedistributionFlags withAutoIncColumn(PEColumn pec) {
		missingAutoIncrement = pec;
		return this;
	}
	
	public RedistributionFlags withExistingAutoInc(Integer i) {
		offsetOfExistingAutoInc = i;
		return this;
	}
	
	public RedistributionFlags withDuplicateKeyClause(List<ExpressionNode> l) {
		onDuplicateKey = l;
		return this;
	}
	
	public RedistributionFlags withRowCount(Boolean v) {
		useRowCount = v;
		return this;
	}
	
	public RedistributionFlags withEnforceScalarValue() {
		mustEnforceScalarValue = true;
		return this;
	}
	
	public RedistributionFlags withInsertIgnore() {
		insertIgnore = true;
		return this;
	}

	public PEColumn getMissingAutoIncrement() {
		return missingAutoIncrement;
	}

	public Integer getOffsetOfExistingAutoInc() {
		return offsetOfExistingAutoInc;
	}

	public List<ExpressionNode> getOnDuplicateKey() {
		return onDuplicateKey;
	}

	public Boolean getUseRowCount() {
		return useRowCount;
	}

	public boolean isMustEnforceScalarValue() {
		return mustEnforceScalarValue;
	}

	public boolean isInsertIgnore() {
		return insertIgnore;
	}
	
	
}
