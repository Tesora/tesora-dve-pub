// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.PETemplate;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.template.jaxb.Template;

public class PEAlterTemplateStatement extends PEAlterStatement<PETemplate> {

	Template body;
	String dbmatch;
	String comment;
	
	public PEAlterTemplateStatement(PETemplate target, String newXML, String newMatch, String newComment) {
		super(target, true);
		if (newXML != null)
			body = PETemplate.build(newXML);
		dbmatch = newMatch;
		comment = newComment;
	}

	@Override
	protected PETemplate modify(SchemaContext sc, PETemplate backing) throws PEException {
		if (body != null)
			backing.setTemplate(body);
		if (dbmatch != null)
			backing.setMatch(dbmatch);
		if (comment != null)
			backing.setComment(comment);		
		return backing;
	}

	public String getNewDefinition() {
		if (body == null)
			return null;
		return PETemplate.build(body);
	}
	
	public String getNewComment() {
		return comment;
	}
	
	public String getNewMatch() {
		return dbmatch;
	}
	
	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext pc) {
		return new CacheInvalidationRecord(getTarget().getCacheKey(),InvalidationScope.LOCAL);
	}

}
