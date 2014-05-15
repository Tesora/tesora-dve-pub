// OS_STATUS: public
package com.tesora.dve.sql.expression;

public enum ScopeParsePhase {
	UNRESOLVING,              // projection
	RESOLVING_CURRENT,        // from clauses
	RESOLVING,                // where clause
	GROUPBY,                  // group by clause
	HAVING,                   // having clause
	TRAILING                  // everything else
}