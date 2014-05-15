// OS_STATUS: public
package com.tesora.dve.sql.node.expression;


import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ObjectUtils;

import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.transform.CopyContext;

public class CharFunctionCall extends FunctionCall {

	private final Name outputEncoding;

	public CharFunctionCall(final List<ExpressionNode> arguments, final Name outputEncoding) {
		super(FunctionName.makeChar(), arguments);
		this.outputEncoding = outputEncoding;
	}

	public Name getOutputEncoding() {
		return this.outputEncoding;
	}

	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		final List<ExpressionNode> argsCopy = new ArrayList<ExpressionNode>(getParameters());
		final CharFunctionCall copy = new CharFunctionCall(argsCopy,
				this.outputEncoding);
		copy.setSetQuantifier(getSetQuantifier());

		return copy;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		if (!super.schemaSelfEqual(other))
			return false;
		CharFunctionCall otherFunctionCall = (CharFunctionCall) other;
		return ObjectUtils.equals(this.outputEncoding, otherFunctionCall.outputEncoding);
	}

	@Override
	protected int selfHashCode() {
		return addSchemaHash(super.selfHashCode(),(outputEncoding == null ? 0 : outputEncoding.hashCode()));
	}
	
}
