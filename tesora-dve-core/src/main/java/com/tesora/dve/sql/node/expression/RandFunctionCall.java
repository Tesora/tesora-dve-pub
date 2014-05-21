// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

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

import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.transform.CopyContext;

public class RandFunctionCall extends FunctionCall {

	private ExpressionNode seed;

	public RandFunctionCall(final ExpressionNode seed) {
		super(FunctionName.makeRand(), seed);
		this.seed = seed;
	}

	public void setSeed(final ExpressionNode seed) {
		this.seed = seed;
	}

	public ExpressionNode getSeed() {
		return this.seed;
	}

	public boolean hasSeed() {
		return this.seed != null;
	}

	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		final ExpressionNode seedCopy = (this.hasSeed()) ? (ExpressionNode) this.seed.copy(cc) : null;
		final RandFunctionCall copy = new RandFunctionCall(seedCopy);
		copy.setSetQuantifier(getSetQuantifier());

		return copy;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		if (!super.schemaSelfEqual(other))
			return false;
		RandFunctionCall rfc = (RandFunctionCall) other;
		if (!this.hasSeed()) {
			return !rfc.hasSeed();
		}

		return this.seed.equals(rfc.seed);
	}

	@Override
	protected int selfHashCode() {
		return addSchemaHash(super.selfHashCode(),(hasSeed() ? seed.hashCode() : 0));
	}

}
