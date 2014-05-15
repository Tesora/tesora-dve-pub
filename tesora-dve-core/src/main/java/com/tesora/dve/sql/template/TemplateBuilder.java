// OS_STATUS: public
package com.tesora.dve.sql.template;

import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.PETemplate;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.template.jaxb.FkModeType;
import com.tesora.dve.sql.template.jaxb.ModelType;
import com.tesora.dve.sql.template.jaxb.RequirementType;
import com.tesora.dve.sql.template.jaxb.TableTemplateType;
import com.tesora.dve.sql.template.jaxb.Template;

public class TemplateBuilder {

	private Template target;
	
	public TemplateBuilder(String name) {
		this(name, null);
	}

	public TemplateBuilder(String name, String match) {
		this(name, match, null);
	}

	public TemplateBuilder(String name, String match, String comment) {
		target = new Template();
		target.setName(name);
		target.setMatch(match);
		target.setComment(comment);
	}
	
	public TemplateBuilder withFKMode(FkModeType fkmt) {
		target.setFkmode(fkmt);
		return this;
	}

	public TemplateBuilder withRequirement(String decl) {
		RequirementType rt = new RequirementType();
		rt.setDeclaration(decl);
		target.getRequirement().add(rt);
		return this;
	}

	public TemplateBuilder withTable(String match, String model) {
		TableTemplateType ttt = new TableTemplateType();
		ttt.setMatch(match);
		ttt.setModel(ModelType.fromValue(model));
		target.getTabletemplate().add(ttt);
		return this;
	}

	public TemplateBuilder withRangeTable(String match, String rangeName, String ...columns) {
		TableTemplateType ttt = new TableTemplateType();
		ttt.setMatch(match);
		ttt.setModel(ModelType.RANGE);
		ttt.setRange(rangeName);
		for(String c : columns)
			ttt.getColumn().add(c);
		target.getTabletemplate().add(ttt);
		return this;
	}
	
	public TemplateBuilder withContainerTable(String match, String container, String ...columns) {
		TableTemplateType ttt = new TableTemplateType();
		ttt.setMatch(match);
		ttt.setModel(ModelType.CONTAINER);
		ttt.setContainer(container);
		for(String c : columns)
			ttt.getDiscriminator().add(c);
		target.getTabletemplate().add(ttt);
		return this;
	}
	
	public String getName() {
		return this.target.getName();
	}

	public String getMatch() {
		return this.target.getMatch();
	}

	public String getComment() {
		return this.target.getComment();
	}

	public String toXml() {
		return PETemplate.build(target);
	}
	
	public Template toTemplate() {
		return target;
	}
	
	public String toCreateRangeStatement(final String rangeName, final String groupName, final Set<Type> columnTypes) {
		final String typeSeparator = ", ";
		final StringBuilder statement = new StringBuilder();
		statement.append("CREATE RANGE IF NOT EXISTS ").append(rangeName).append(" (");
		for (final Type type : columnTypes) {
			statement.append(type.getTypeName()).append(typeSeparator);
		}
		final int statementLength = statement.length();
		statement.delete(statementLength - typeSeparator.length(), statementLength).append(") PERSISTENT GROUP ").append(groupName);
		return statement.toString();
	}
	
	public String toCreateStatement() {
		StringBuilder buf = new StringBuilder(getCreateStatement(this.getName(), this.toXml()));
		emitOptionalFields(buf);
		return buf.toString();
	}
	
	public String toAlterStatement() {
		StringBuilder buf = new StringBuilder(getAlterStatement(this.getName(), this.toXml()));
		emitOptionalFields(buf);
		return buf.toString();
	}

	public static String getClassPathCreate(String templateName) throws PEException {
		String body = StringEscapeUtils.escapeSql(PEFileUtils.readToString(TemplateBuilder.class, "/templates/" + templateName + ".template"));
		StringBuilder buf = new StringBuilder(getCreateStatement(templateName, body));
		return buf.toString();
	}

	private static String getCreateStatement(final String name, final String xmlBody) {
		return "CREATE TEMPLATE IF NOT EXISTS " + name + " XML='" + xmlBody + "'";
	}

	private static String getAlterStatement(final String name, final String xmlBody) {
		return "ALTER TEMPLATE " + name + " SET XML='" + xmlBody + "'";
	}

	private void emitOptionalFields(final StringBuilder buf) {
		final String match = this.getMatch();
		if (StringUtils.isNotBlank(match)) {
			buf.append(" MATCH='").append(match).append("'");
		}

		final String comment = this.getComment();
		if (StringUtils.isNotBlank(comment)) {
			buf.append(" COMMENT='").append(comment).append("'");
		}
	}
}
