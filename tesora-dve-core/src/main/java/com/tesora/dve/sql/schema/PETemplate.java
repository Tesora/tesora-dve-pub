// OS_STATUS: public
package com.tesora.dve.sql.schema;

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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.PEXmlUtils;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.PersistentTemplate;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.cache.CacheSegment;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.template.jaxb.TableTemplateType;
import com.tesora.dve.sql.template.jaxb.Template;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;

public class PETemplate extends Persistable<PETemplate, PersistentTemplate> {

	private Template definition;
	private String comment;
	private String dbmatch;
	private Pattern dbMatchPattern;
	
	private ListOfPairs<Pattern, TableTemplateType> templates = new ListOfPairs<Pattern, TableTemplateType>();
	
	public PETemplate(SchemaContext sc, UnqualifiedName name, String body, String anyMatch, String anyComment) {
		super(getTemplateKey(name));
		setName(name);

		setTemplate(build(body));
		
		setComment(anyComment, definition.getComment());
		setMatch(anyMatch, definition.getMatch());

		setPersistent(sc,null,null);
	}
	
	private PETemplate(PersistentTemplate pt, SchemaContext pc) {
		super(getTemplateKey(pt.getName()));
		pc.startLoading(this, pt);
		setName(new UnqualifiedName(pt.getName()));

		setTemplate(build(pt.getDefinition()));

		setComment(pt.getComment(), definition.getComment());
		setMatch(pt.getMatch(), definition.getMatch());

		setPersistent(pc,pt,pt.getId());
		pc.finishedLoading(this, pt);
	}
	
	public static PETemplate load(PersistentTemplate pt, SchemaContext pc) {
		PETemplate t = (PETemplate)pc.getLoaded(pt,getTemplateKey(pt.getName()));
		if (t == null)
			t = new PETemplate(pt,pc);
		return t;
	}
		
	public Template getTemplate() {
		return definition;
	}
	
	public void setTemplate(Template t) {
		definition = t;
		
		templates = new ListOfPairs<Pattern,TableTemplateType>();
		for(TableTemplateType ttt : definition.getTabletemplate()) {
			Pattern p = Pattern.compile(ttt.getMatch());
			templates.add(p,ttt);			
		}
	}
	
	public String getDefinition() {
		return build(definition);
	}
	
	public String getComment() {
		return comment;
	}
	
	public void setComment(String c, String cFromTemplate) {
		setComment(StringUtils.isBlank(c) ? cFromTemplate : c);
	}

	public void setComment(String c) {
		comment = c;
	}
	
	public String getMatch() {
		return dbmatch;
	}
	
	public void setMatch(String c, String cFromTemplate) {
		setMatch(StringUtils.isBlank(c) ? cFromTemplate : c);
	}

	public void setMatch(String c) {
		dbmatch = c;
		dbMatchPattern = dbmatch != null ? Pattern.compile(dbmatch) : null;
	}
	
	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return PersistentTemplate.class;
	}

	@Override
	protected int getID(PersistentTemplate p) {
		return p.getId();
	}

	@Override
	protected PersistentTemplate lookup(SchemaContext sc) throws PEException {
		return null;
	}

	@Override
	protected PersistentTemplate createEmptyNew(SchemaContext sc)
			throws PEException {
		return new PersistentTemplate(getName().getUnquotedName().get(), build(definition), dbmatch, comment);
	}

	@Override
	protected void populateNew(SchemaContext sc, PersistentTemplate p)
			throws PEException {
	}

	@Override
	protected void updateExisting(SchemaContext sc, PersistentTemplate p) throws PEException {
		p.setDefinition(getDefinition());
		p.setMatch(("".equals(dbmatch) ? null : dbmatch));
		p.setComment(("".equals(comment) ? null : comment));
	}

	@Override
	public Persistable<PETemplate, PersistentTemplate> reload(SchemaContext toContext) throws PEException {
		PersistentTemplate pt = toContext.getCatalog().findTemplate(getName().get());
		if (pt == null) return null;
		return PETemplate.load(pt, toContext);
	}

	
	@Override
	protected Persistable<PETemplate, PersistentTemplate> load(
			SchemaContext sc, PersistentTemplate p) throws PEException {
		return new PETemplate(sc, new UnqualifiedName(p.getName()), p.getDefinition(), p.getMatch(), p.getComment());
	}

	@Override
	protected String getDiffTag() {
		return "Template";
	}

	public TableTemplateType findMatch(String tableName) {
		for(Pair<Pattern, TableTemplateType> p : templates) {
			Matcher m = p.getFirst().matcher(tableName);
			if (m.matches())
				return p.getSecond();
		}
		return null;
	}

	public boolean isMatch(UnqualifiedName dbName) {
		if (dbMatchPattern == null) return false;
		String str = dbName.getUnquotedName().get();
		Matcher m = dbMatchPattern.matcher(str);
		return m.matches();
	}
	
	public static Template build(String in) {
		try {
			return PEXmlUtils.unmarshalJAXB(in, Template.class);
		} catch (PEException pe) {
			throw new SchemaException(Pass.SECOND, "Unable to unmarshall template definition",pe);
		}
	}
	
	public static String build(Template t) {
		try {
			return PEXmlUtils.marshalJAXB(t);
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to marshall template definition",pe);
		}
	}
	
	public static SchemaCacheKey<PETemplate> getTemplateKey(UnqualifiedName n) {
		return getTemplateKey(n.getUnquotedName().get());
	}
	
	public static SchemaCacheKey<PETemplate> getTemplateKey(String n) {
		return new TemplateCacheKey(n);
	}
	
	private static class TemplateCacheKey extends SchemaCacheKey<PETemplate> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		String name;
		
		public TemplateCacheKey(String n) {
			name = n;
		}

		@Override
		public CacheSegment getCacheSegment() {
			return CacheSegment.TEMPLATE;
		}
			
		@Override
		public int hashCode() {
			return initHash(PETemplate.class,name.hashCode());
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof TemplateCacheKey) {
				TemplateCacheKey oct = (TemplateCacheKey) o;
				return name.equals(oct.name);
			}
			return false;
		}

		@Override
		public PETemplate load(SchemaContext sc) {
			PersistentTemplate ss = sc.getCatalog().findTemplate(name);
			if (ss == null)
				return null;
			return PETemplate.load(ss, sc);
		}

		@Override
		public String toString() {
			return "PETemplate:" + name;
		}
		
	}
}
