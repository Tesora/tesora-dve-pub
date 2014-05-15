// OS_STATUS: public
package com.tesora.dve.common.catalog;

import java.util.Collections;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.annos.TableView;

@InfoSchemaTable(logicalName="persistent_template",
views={@TableView(view=InfoView.SHOW, name="template", pluralName="templates", 
		columnOrder={ShowSchema.Template.NAME, ShowSchema.Template.MATCH, ShowSchema.Template.COMMENT, ShowSchema.Template.BODY}, extension=true),
       @TableView(view=InfoView.INFORMATION, name="templates", pluralName="", 
       columnOrder={"name", "dbmatch", "definition", "template_comment"}, extension=true)})
@Entity
@Table(name="template")
public class PersistentTemplate implements CatalogEntity, NamedCatalogEntity {

	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column(name = "template_id")
	private int id;

	@Column(name = "name", nullable = false)
	private String name;
	
	@Column(name = "definition", nullable = false)
	@Lob
	String definition;

	@Column(name = "dbmatch", nullable = true)
	String dbmatch;
	
	@Column(name = "template_comment", nullable = true)
	String comment;
	
	@Override
	public int getId() {
		// TODO Auto-generated method stub
		return id;
	}

	public PersistentTemplate() {
		
	}
	
	public PersistentTemplate(String name, String def, String match, String comment) {
		this.name = name;
		this.definition = def;
		this.dbmatch = match;
		this.comment = comment;
	}
	
	@InfoSchemaColumn(logicalName="name",fieldName="name",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.Template.NAME,orderBy=true,ident=true),
			       @ColumnView(view=InfoView.INFORMATION, name="name", orderBy=true, ident=true)})
	@Override
	public String getName() {
		return name;
	}

	@InfoSchemaColumn(logicalName="body",fieldName="definition",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.Template.BODY,orderBy=true,ident=true),
			       @ColumnView(view=InfoView.INFORMATION, name="definition", orderBy=true, ident=true)})
	public String getDefinition() {
		return definition;
	}
	
	public void setDefinition(String def) {
		definition = def;
	}

	
	@InfoSchemaColumn(logicalName="dbmatch",fieldName="dbmatch",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.Template.MATCH),
			       @ColumnView(view=InfoView.INFORMATION, name="dbmatch")})
	public String getMatch() {
		return dbmatch;
	}
	
	public void setMatch(String def) {
		dbmatch = def;
	}

	@InfoSchemaColumn(logicalName="comment", fieldName="comment",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.Template.COMMENT),
				   @ColumnView(view=InfoView.INFORMATION, name="template_comment")})
	public String getComment() {
		return comment;
	}
	
	public void setComment(String v) {
		comment = v;
	}
	
	
	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo)
			throws PEException {
		return null;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo)
			throws PEException {
		return null;
	}

	@Override
	public void removeFromParent() throws Throwable {
		
	}

	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c)
			throws Throwable {
		return Collections.emptyList();
	}

	@Override
	public void onUpdate() {
		
	}

	@Override
	public void onDrop() {
		
	}

}
