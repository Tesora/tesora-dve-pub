// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

public enum ExecutionType {

	INSERT,
	UPDATE,
	DELETE,
	SELECT,
	PARALLEL,
	SEQUENCE,
	DDL,
	DDLQUERY,
	SESSION,
	TRANSACTION,
	RAW,
	UNION,
	PREPARE
	
}
