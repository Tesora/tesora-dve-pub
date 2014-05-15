// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.db.mysql.common.DBTypeBasedUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.PlanningResult;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;

public class ExecutePStmtStatement extends PStmtStatement {

	private List<VariableInstance> vars;
	
	public ExecutePStmtStatement(UnqualifiedName name, List<VariableInstance> vars) {
		super(name);
		this.vars = vars;
	}
	
	
	@Override
	public void plan(SchemaContext sc, ExecutionSequence es) throws PEException {
		List<String> values = new ArrayList<String>();
		for(VariableInstance vi : vars) {
			String v = sc.getConnection().getVariableValue(vi.buildAccessor());
			if (v == null)
				values.add(null);
			else
				values.add(DBTypeBasedUtils.escape(v));
		}
		PlanningResult pr = InvokeParser.bindPreparedStatement(sc, getName().get(), values);
		ExecutionPlan ep = pr.getPlans().get(0);
		es.setSteps(ep.getSequence().getSteps());
	}
}
