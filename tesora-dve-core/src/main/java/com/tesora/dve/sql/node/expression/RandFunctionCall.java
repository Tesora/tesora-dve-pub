// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

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
