// OS_STATUS: public
package com.tesora.dve.sql.infoschema.engine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.AbstractInformationSchemaColumnView;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class LogicalQuery {
	
	protected ViewQuery orig;
	
	protected SelectStatement stmt;
	protected Map<String,Object> params;
	protected Map<TableKey,ExpressionNode> viewToLogicalForwarding;
	protected List<List<AbstractInformationSchemaColumnView>> projectionColumns;
	
	public LogicalQuery(ViewQuery basedOn, SelectStatement s, Map<String,Object> p, Map<TableKey, ExpressionNode> forwarding,
			List<List<AbstractInformationSchemaColumnView>> projCols) {
		stmt = s;
		params = p;
		if (params.isEmpty())
			params = new HashMap<String,Object>();
		this.viewToLogicalForwarding = forwarding;
		orig = basedOn;
		projectionColumns = projCols;
	}
	
	public LogicalQuery(LogicalQuery base, SelectStatement ss) {
		stmt = ss;
		params = base.params;
		viewToLogicalForwarding = base.viewToLogicalForwarding;
		orig = base.orig;
		projectionColumns = base.projectionColumns;
	}
	
	public SelectStatement getQuery() { return stmt; }
	public Map<String,Object> getParams() { return params; }
	public Map<TableKey,ExpressionNode> getForwarding() { return viewToLogicalForwarding; }
	public List<List<AbstractInformationSchemaColumnView>> getProjectionColumns() { return projectionColumns; }
	
	public ViewQuery getViewQuery() {
		return orig;
	}
}