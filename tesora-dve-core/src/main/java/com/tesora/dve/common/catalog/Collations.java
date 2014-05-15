// OS_STATUS: public
package com.tesora.dve.common.catalog;

import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.apache.commons.lang.BooleanUtils;

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.annos.TableView;

@InfoSchemaTable(logicalName = "collations", views = {
		@TableView(view = InfoView.SHOW, name = "collation", pluralName = "", 
				columnOrder = { ShowSchema.Collation.NAME,
								ShowSchema.Collation.CHARSET_NAME,
								ShowSchema.Collation.ID,
								ShowSchema.Collation.DEFAULT,
								ShowSchema.Collation.COMPILED,
								ShowSchema.Collation.SORTLEN }),
		@TableView(view = InfoView.INFORMATION, name = "collations", pluralName = "", columnOrder = {
				"COLLATION_NAME", "CHARACTER_SET_NAME", "ID", "IS_DEFAULT", "IS_COMPILED", "SORTLEN"
		}) })
@Entity
@Table(name="collations")
public class Collations implements CatalogEntity {

	private static final long serialVersionUID = 1L;
	
	@Column(name="name",columnDefinition="varchar(32)",nullable=false)
	String name;
	
	@Column(name="character_set_name",columnDefinition="varchar(32)",nullable=false)
	String characterSetName;

	@Id
	@Column(name="id",columnDefinition="int(11)",nullable=false)
	int id;

	@Column(name="is_default",columnDefinition="int(11) default '0'",nullable=false)
	int isDefault;
	
	@Column(name="is_compiled",columnDefinition="int(11) default '1'",nullable=false)
	int isCompiled;

	@Column(name="sortlen",columnDefinition="bigint(3)",nullable=false)
	long sortlen;

	public Collations(String name, String characterSetName, int id, boolean isDefault, boolean isCompiled, long sortlen) {
		this.name = name;
		this.characterSetName = characterSetName;
		this.id = id;
		this.isDefault = BooleanUtils.toInteger(isDefault);
		this.isCompiled = BooleanUtils.toInteger(isCompiled);
		this.sortlen = sortlen;
	}

	public Collations() {
	}
	
	@InfoSchemaColumn(logicalName = "name", fieldName = "name", sqlType = java.sql.Types.VARCHAR, sqlWidth = 32, 
			views = {@ColumnView(view = InfoView.SHOW, name = ShowSchema.Collation.NAME, orderBy = true, ident = true),
					 @ColumnView(view = InfoView.INFORMATION, name = "COLLATION_NAME", orderBy = true, ident = true) })
	public String getName() {
		return name;
	}

	@InfoSchemaColumn(logicalName="character_set_name", fieldName="characterSetName",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=32,
			views={@ColumnView(view = InfoView.SHOW, name = ShowSchema.Collation.CHARSET_NAME ),
				   @ColumnView(view=InfoView.INFORMATION, name="CHARACTER_SET_NAME")})
	public String getCharacterSetName() {
		return characterSetName;
	}
	
	@Override
	@InfoSchemaColumn(logicalName="id", fieldName="id",
			sqlType=java.sql.Types.INTEGER,
			views={@ColumnView(view=InfoView.SHOW, name = ShowSchema.Collation.ID),
				   @ColumnView(view=InfoView.INFORMATION, name="ID")})
	public int getId() {
		return id;
	}

	@InfoSchemaColumn(logicalName="is_default", fieldName="isDefault",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=3,
			views={@ColumnView(view=InfoView.SHOW, name = ShowSchema.Collation.DEFAULT),
				   @ColumnView(view=InfoView.INFORMATION, name="IS_DEFAULT")},
			booleanStringTrueFalseValue={"YES", ""})
	public boolean getIsDefault() {
		return BooleanUtils.toBoolean(isDefault);
	}

	@InfoSchemaColumn(logicalName="is_compiled", fieldName="isCompiled",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=60,
			views={@ColumnView(view=InfoView.SHOW, name = ShowSchema.Collation.COMPILED),
				   @ColumnView(view=InfoView.INFORMATION, name="IS_COMPILED")},
			booleanStringTrueFalseValue={"YES", ""})
	public boolean getIsCompiled() {
		return BooleanUtils.toBoolean(isCompiled);
	}

	@InfoSchemaColumn(logicalName="sortlen", fieldName="sortlen",
			sqlType=java.sql.Types.INTEGER,
			views={@ColumnView(view=InfoView.SHOW, name = ShowSchema.Collation.SORTLEN),
				   @ColumnView(view=InfoView.INFORMATION, name="SORTLEN")})
	public long getSortlen() {
		return sortlen;
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo)
			throws PEException {
		// no show
		return null;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo)
			throws PEException {
		// no show
		return null;
	}

	@Override
	public void removeFromParent() throws Throwable {
		// no parents
		
	}

	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c)
			throws Throwable {
		// no dependents
		return null;
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}
	
}
