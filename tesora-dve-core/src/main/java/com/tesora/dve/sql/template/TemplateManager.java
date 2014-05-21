// OS_STATUS: public
package com.tesora.dve.sql.template;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.catalog.FKMode;
import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.ContainerDistributionVector;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEContainer;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.PETemplate;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.RangeDistribution;
import com.tesora.dve.sql.schema.RangeDistributionVector;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.VectorRange;
import com.tesora.dve.sql.schema.cache.CacheType;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.PECreateStatement;
import com.tesora.dve.sql.template.jaxb.RequirementType;
import com.tesora.dve.sql.template.jaxb.TableTemplateType;
import com.tesora.dve.sql.util.Pair;

public final class TemplateManager {

	private TemplateManager() {
	}
	
	public static Pair<Name, TemplateMode> findTemplateForDatabase(final SchemaContext sc, final Name dbName, final Name templateName,
			final TemplateMode templateMode) {
		if (dbName == null) {
			throw new NullPointerException("database name");
		}
		
		if (templateName != null) {
			// if specified, ensure the template exists
			if (hasTemplate(sc, templateName)) {
				return new Pair<Name, TemplateMode>(templateName, templateMode);
			}

			throw new SchemaException(Pass.SECOND, "No such template: '" + templateName.getSQL() + "'");
		} else {
			// not specified, look for a match
			final Name matchedTemplate = findTemplateMatchForDatabase(sc, dbName);
			if ((matchedTemplate != null) || !templateMode.requiresTemplate()) {
				return new Pair<Name, TemplateMode>(matchedTemplate, templateMode);
			}

			throw new SchemaException(Pass.SECOND, "Template required, but not matched");
		}
	}

	public static Name findTemplateMatchForDatabase(final SchemaContext sc, final Name dbName) {
		final List<PETemplate> matchingTemplates = sc.findMatchTemplates();
		for (PETemplate pet : matchingTemplates) {
			if (pet.isMatch(dbName.getUnqualified())) {
				return pet.getName().getUnquotedName();
			}
		}
		return null;
	}

	public static boolean hasTemplate(final SchemaContext sc, final Name templateName) {
		return (sc.findTemplate(templateName) != null);
	}

	public static PETemplate findTemplate(SchemaContext sc, String templateName) {
		if (templateName != null) {
			return sc.findTemplate(new UnqualifiedName(templateName));
		}

		return null;
	}
	
	public static boolean inject(SchemaContext sc, PEDatabase peds, PETable tab) {
		if (tab.getDistributionVector(sc) != null)
			return false;
		PETemplate pet = findTemplate(sc,peds.getTemplateName());
		if (pet == null)
			return false;
		TableTemplateType ttt = pet.findMatch(tab.getName().get());
		if (ttt == null)
			return false;
		ArrayList<PEColumn> columns = new ArrayList<PEColumn>();
		for(String cname : ttt.getColumn()) {
			PEColumn c = tab.lookup(sc, cname);
			if (c == null)
				throw new SchemaException(Pass.SECOND,"Missing column for inject distribution vector on table " + tab.getName().getSQL() + ": " + cname);
			columns.add(c);
		}
		DistributionVector.Model model = DistributionVector.Model.getModelFromPersistent(ttt.getModel().value());
		if (model != null) {
			DistributionVector dv = null;
			if (model == DistributionVector.Model.RANGE) {
				String rangeName = ttt.getRange(); 
				if (rangeName == null)
					throw new SchemaException(Pass.SECOND, "Malformed table template - range model specified but no range specified");
				RangeDistribution rd = sc.findRange(new UnqualifiedName(rangeName), tab.getPersistentStorage(sc).getName());
				if (rd == null)
					throw new SchemaException(Pass.SECOND,"No such range from template '" + peds.getTemplateName() + "' on storage group " + tab.getPersistentStorage(sc).getName() + ": " + rangeName);
				dv = new RangeDistributionVector(sc, columns, false, new VectorRange(sc,rd));
			} else if (model == DistributionVector.Model.CONTAINER) {
				String containerName = ttt.getContainer(); 
				if (!StringUtils.isBlank(containerName)) {
					PEContainer container = sc.findContainer(new UnqualifiedName(containerName));
					if (container == null) {
						throw new SchemaException(Pass.SECOND, "No such container from template '" + peds.getTemplateName() + "': " + containerName);
					}
					dv = new ContainerDistributionVector(sc,container,false);
					if (ttt.getDiscriminator().size() > 0) {
						for(int i = 0; i < ttt.getDiscriminator().size(); i++) {
							UnqualifiedName un = new UnqualifiedName(ttt.getDiscriminator().get(i));
							PEColumn pec = tab.lookup(sc, un);
							if (pec == null)
								throw new SchemaException(Pass.SECOND, "No such column: " + un.getSQL() + " - cannot build discriminator");
							pec.setContainerDistributionValuePosition(i + 1);
						}
						container.setBaseTable(sc, tab);
					}
				}
			} else {
				dv = new DistributionVector(sc,columns,model);
			}
			tab.setDistributionVector(sc,dv);
		}
		return true;
	}

	public static List<Statement> adaptPrereqs(SchemaContext pc, PEDatabase peds) throws PEException {
		if (peds.getTemplateName() == null)
			return Collections.emptyList();
		PETemplate pet;
		try {
			pet = findTemplate(pc, peds.getTemplateName());
		} catch (Exception e) {
			throw new PEException("Error finding template", e);
		}
		if (pet == null)
			return Collections.emptyList();
		if (pet.getTemplate().getFkmode() != null) {
			FKMode obj = FKMode.toMode(pet.getTemplate().getFkmode().value());
			if (peds.getFKMode() == null)
				peds.setFKMode(obj);
		}
		ArrayList<Statement> ret = new ArrayList<Statement>();
		// we can reuse the persistence context, but make sure we change the options
		SchemaContext usepc = pc;
		ParserOptions prevOptions = usepc.getOptions();
		ParserOptions subOptions = prevOptions.setAllowDuplicates();
		try {
			for(RequirementType rt : pet.getTemplate().getRequirement()) {
				String raw = rt.getDeclaration();
				String sqlcommand = raw.replaceAll("#sg#", peds.getDefaultStorage(pc).getName().get());
				List<Statement> prereq = InvokeParser.parse(InvokeParser.buildInputState(sqlcommand,usepc), subOptions, usepc).getStatements();
				for(Statement s : prereq) {
					if (s instanceof PECreateStatement) {
						PECreateStatement<?,?> pecs = (PECreateStatement<?, ?>) s;
						if (pecs.isNew()) {
							ret.add(s);
							if (usepc.getSource().getType() == CacheType.MUTABLE) {
								Persistable<?,?> targ = pecs.getRoot();
								SchemaCacheKey<?> sck = targ.getCacheKey();
								if (sck != null)
									usepc.getSource().setLoaded(targ, sck);
							}
						}
					} else {
						ret.add(s);
					}
				}
			}
		} finally {
			usepc.setOptions(prevOptions);
		}
		return ret;
	}
}
