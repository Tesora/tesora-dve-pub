package com.tesora.dve.common.catalog;

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


import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.log4j.Logger;

import com.tesora.dve.common.PECryptoUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.worker.UserAuthentication;

@Entity
@Table(name="user")
public class User implements CatalogEntity {

	private static Logger logger = Logger.getLogger(User.class);

	private static final long serialVersionUID = 1L;
	
	@Id
	@GeneratedValue
	@Column( name="id" )
	int id;

	@Column(name="name")
	String name;
	
	@Column(name="accessSpec")
	String accessSpec;
	
	@Column(name="password")
	String password;
	
	@Column(name="grantPriv")
	Boolean grantPriv;
	
	@Transient
	String decryptedPassword;

	@Column(name="admin_user")
	Boolean adminUser = false;
	
	@OneToMany(cascade=CascadeType.ALL)
	@JoinColumn(name="user_id")
	Set<Priviledge> priviledges = new HashSet<Priviledge>();

	
	private transient ColumnSet showColumnSet = null;

	public User(String name, String password, String accessSpec) {
		this(name, password, accessSpec, false);
	}

	public User(String name, String password, String accessSpec, boolean adminUser) {
		this.name = name;
		this.accessSpec = accessSpec;

		setAdminUser(adminUser);
		setPlaintextPassword(password);
	}

	public User(Properties props, String prefix) {
		this.name = props.getProperty(prefix + "user");
		setPlaintextPassword(props.getProperty(prefix + "password"));
		if (props.containsKey(prefix + "accessSpec"))
			this.accessSpec = props.getProperty(prefix + "accessSpec");
		else 
			this.accessSpec = "%";
	}

	User() {
	}

	@Override
	public int getId() {
		return id;
	}

	public String getName() {
		return this.name;
	}
	
	public String getPassword() {
		return this.password;
	}
	
	public String getAccessSpec() {
		return this.accessSpec;
	}
	
	public boolean getGrantPriv() {
		return grantPriv == null ? false : grantPriv;
	}

	public void setGrantPriv(boolean grantPriv) {
		this.grantPriv = grantPriv;
	}

	public String getPlaintextPassword() {
		if(decryptedPassword == null) {
			try {
				decryptedPassword = PECryptoUtils.decrypt(password);
			} catch(Exception e) {
				logger.error("Failed to decrypt password for user '" + name + "'", e);

				decryptedPassword = password;
			}
		}
		return decryptedPassword;
	}

	public Boolean getAdminUser() {
		return adminUser;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setAccessSpec(String accessSpec) {
		this.accessSpec = accessSpec;
	}
	
	public void setPlaintextPassword(String password) {
		decryptedPassword = password;

		try {
			this.password = PECryptoUtils.encrypt(password);
		} catch (Exception e) {
			logger.error("Failed to encrypt password for user '" + name + "'", e);
			this.password = password;
		}
	}

	public void setAdminUser(Boolean adminUser) {
		this.adminUser = adminUser;
		
		if(adminUser) {
			setGrantPriv(true);
		}
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo) {
		if ( showColumnSet == null ) {
			showColumnSet = new ColumnSet();
			showColumnSet.addColumn("User Name", 255, "varchar", Types.VARCHAR);
		}
		return showColumnSet;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo)
			throws PEException {
		ResultRow rr = new ResultRow();
		rr.addResultColumn(this.name, false);
		
		return rr;
	}

	@Override
	public void removeFromParent() throws Throwable {
	}

	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c)
			throws Throwable {
		// privileges are dependents
		List<CatalogEntity> out = new ArrayList<CatalogEntity>();
		c.refresh(this);
		out.addAll(priviledges);
		return out;
	}

	public Priviledge findPriviledge(UserDatabase ondb, Tenant ten) {
		for(Priviledge p : priviledges) {
			if (p.matches(ondb,ten))
				return p;
		}
		return null;
	}
	
	public Set<Priviledge> getPriviledges() {
		return priviledges;
	}

	public void addPriviledge(Priviledge p) {
		priviledges.add(p);
	}

	public UserAuthentication getAuthentication() {
		return new UserAuthentication(getName(), getPlaintextPassword(), getAdminUser());
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}
	
}
