package com.tesora.dve.sql.infoschema.engine;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import com.tesora.dve.db.NativeType;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class LogicalQuery {

	protected ViewQuery orig;
	
	protected SelectStatement stmt;
	protected Map<String,Object> params;

	public LogicalQuery(ViewQuery orig, SelectStatement xlated, Map<String,Object> p) {
		this.orig = orig;
		this.stmt = xlated;
		params = p;
		if (params.isEmpty())
			params = new HashMap<String,Object>();
	}
	
	public SelectStatement getQuery() { return stmt; }
	public Map<String,Object> getParams() { return params; }

	public ViewQuery getViewQuery() {
		return orig;
	}

	public static void buildNativeType(ColumnSet cs, String colName, String colAlias, Object in) throws PEException {
		if (in instanceof String) {
            NativeType nt = Singletons.require(HostService.class).getDBNative().getTypeCatalog().findType(java.sql.Types.VARCHAR, true);
			cs.addColumn(colAlias, 255, nt.getTypeName(), java.sql.Types.VARCHAR);
		} else if (in instanceof BigInteger) {
            NativeType nt = Singletons.require(HostService.class).getDBNative().getTypeCatalog().findType(java.sql.Types.BIGINT, true);
			cs.addColumn(colAlias, 32, nt.getTypeName(), java.sql.Types.BIGINT);
		} else {
			throw new PEException("Fill me in: type guess for result column type: " + in);
		}
	}

	
}
