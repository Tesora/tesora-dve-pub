// OS_STATUS: public
package com.tesora.dve.common.catalog;


import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.hibernate.annotations.ForeignKey;

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.annos.TableView;

@InfoSchemaTable(logicalName = "container_tenant", views = {
		@TableView(view = InfoView.SHOW, name = "container_tenant", pluralName = "container_tenants", columnOrder = {
				ShowSchema.ContainerTenant.CONTAINER, ShowSchema.ContainerTenant.NAME,
				ShowSchema.ContainerTenant.ID })
//		,
//		@TableView(view = InfoView.INFORMATION, name = "container_tenant", pluralName = "", columnOrder = {
//				"container", "discriminant", "id"}) 
				})
@Entity
@Table(name = "container_tenant") 
//@org.hibernate.annotations.Table(appliesTo="container_tenant",
//		indexes={@org.hibernate.annotations.Index(name="cont_ten_idx", columnNames = { "container_id", "discriminant" })})
public class ContainerTenant implements ITenant {

	private static final long serialVersionUID = 1L;

	// the global tenant is what the container tenant is set to when the user does a use container global
	public static final ContainerTenant GLOBAL_CONTAINER_TENANT = new ContainerTenant(null,null) {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public boolean isGlobalTenant() {
			return true;
		}
	};
	
	@Id
	@GeneratedValue
	@Column(name = "ctid")
	int id;

	@ForeignKey(name="fk_cont_tenant_cont")
	@ManyToOne
	@JoinColumn(name = "container_id", nullable=false)
	Container container;

	@Column(name = "discriminant", nullable=false)
	@Lob
	String discriminant;

	// should we have a separate container tenant id?  or would we just update the ctid?
	
	public ContainerTenant(Container ofContainer, String valueRep) {
		container = ofContainer;
		discriminant = valueRep;
	}
	
	ContainerTenant() {
		
	}
	
	@InfoSchemaColumn(logicalName="id", fieldName="id",
			sqlType=java.sql.Types.INTEGER,
			views={@ColumnView(view=InfoView.SHOW,name=ShowSchema.ContainerTenant.ID),
				   @ColumnView(view=InfoView.INFORMATION,name="id")})
	@Override
	public int getId() {
		return id;
	}

	@InfoSchemaColumn(logicalName="container",fieldName="container",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW,name=ShowSchema.ContainerTenant.CONTAINER),
				   @ColumnView(view=InfoView.INFORMATION,name="container")})
	public Container getContainer() {
		return container;
	}
	
	@InfoSchemaColumn(logicalName="name",fieldName="discriminant",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.ContainerTenant.NAME,orderBy=true,ident=true),
			       @ColumnView(view=InfoView.INFORMATION, name="discriminant", orderBy=true, ident=true)})
	public String getDiscriminant() {
		return discriminant;
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
		return null;
	}

	@Override
	public boolean isGlobalTenant() {
		return false;
	}

	@Override
	public String getUniqueIdentifier() {
		return discriminant;
	}

	@Override
	public boolean isPersistent() {
		// not always true - the global tenant is not an actual tenant
		return !isGlobalTenant();
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}
	
}
