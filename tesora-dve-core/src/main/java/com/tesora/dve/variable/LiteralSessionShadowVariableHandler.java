// OS_STATUS: public
package com.tesora.dve.variable;

public class LiteralSessionShadowVariableHandler extends
		SessionShadowVariableHandler {

	@Override
	public String getSessionAssignmentClause(String name, String value) {
		return name + " = '" + value + "'";
	}
}
