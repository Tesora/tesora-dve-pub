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

import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.commons.lang.BooleanUtils;
import org.apache.log4j.Logger;

import org.hibernate.annotations.ForeignKey;

import com.tesora.dve.common.PECryptoUtils;
import com.tesora.dve.common.SiteInstanceStatus;
import com.tesora.dve.exceptions.PEAlreadyExistsException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.worker.UserAuthentication;

@Entity
@Table(name="site_instance")
public class SiteInstance implements ISiteInstance {
	private static Logger logger = Logger.getLogger(SiteInstance.class);

	private static final long serialVersionUID = 1L;
	
	@Id
	@GeneratedValue
	int id;
	
	@Column( name="name", nullable=false, unique=true )
	String name;
	
	@ForeignKey(name="fk_site_instance_site")
	@ManyToOne
	@JoinColumn(name="storage_site_id")
	PersistentSite storageSite;
	
	@Column( name="instance_url", nullable=false )
	String instanceURL;
	
	@Column( name="is_master", nullable = false)
	int isMaster;

	@Enumerated(EnumType.STRING)
	@Column( name="status", nullable = false)
	SiteInstanceStatus status;
	
	@Column(name="user", nullable = false)
	String user;
	
	@Column(name="password", nullable = false)
	String password;
	
	@Transient
	String decryptedPassword;

	public SiteInstance() {
		super();
	}

	public SiteInstance(String name, String url, String user, String password) {
		this(name, url, user, password, false, SiteInstanceStatus.ONLINE.name());
	}

	public SiteInstance(String name, String url, String user, String password, boolean isMaster, String status) {
		this.name = name;
		this.instanceURL = url;
		this.user = user;
		
		setDecryptedPassword(password);
		setMaster(isMaster);
		setStatus(status);
	}

	public void setStorageSite(PersistentSite storageSite) throws PEAlreadyExistsException {
		if (this.storageSite != null)
			throw new PEAlreadyExistsException("SiteInstance " + this + " is already in " + storageSite.getClass().getSimpleName()
					+ " " + storageSite);
		this.storageSite = storageSite;
	}

	public void clearStorageSite() {
		this.storageSite = null;
	}
	
	public PersistentSite getStorageSite() {
		return storageSite;
	}

	public String getInstanceURL() {
		return instanceURL;
	}

	public void setInstanceURL(String instanceURL) {
		this.instanceURL = instanceURL;
	}
	
	public String getUser() {
		return user;
	}
	
	public void setUser(String user) {
		this.user = user;
	}
	
	public String getEncryptedPassword() {
		return password;
	}
	
	public void setEncryptedPassword(String password) {
		this.password = password;
	}
	
	public UserAuthentication getAuthentication() {
		return new UserAuthentication(user, getDecryptedPassword(), false);
	}
	
	public String getDecryptedPassword() {
		if(decryptedPassword == null) {
			try {
				decryptedPassword = PECryptoUtils.decrypt(password);
			} catch(Exception e) {
				logger.error("Failed to decrypt password for site instance '" + name + "'", e);

				decryptedPassword = password;
			}
		}
		return decryptedPassword;	
	}
	
	public void setDecryptedPassword(String password) {
		decryptedPassword = password;

		try {
			this.password = PECryptoUtils.encrypt(password);
		} catch (Exception e) {
			logger.error("Failed to encrypt password for site instance '" + name + "'", e);
			this.password = password;
		}	
	}

	@Override
	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Boolean schemaIsMaster() {
		return BooleanUtils.toBooleanObject(isMaster);
	}
	public boolean isMaster() {
		return isMaster == 1;
	}

	public void setMaster(boolean isMaster) {
		this.isMaster = isMaster ? 1 : 0;
	}

	public String getStatus() {
		return status.name();
	}
	
	public boolean isEnabled() {
		return status == SiteInstanceStatus.ONLINE;
	}
	
	public void failSiteInstance() {
		this.status = SiteInstanceStatus.FAILED;
	}

	public void setStatus(String status) {
		this.status = SiteInstanceStatus.fromString(status);
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
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SiteInstance other = (SiteInstance) obj;
		if (id != other.id)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "SiteInstance [name=" + name + ", url=" + instanceURL + ", user=" + user 
				+ ", isMaster=" + isMaster + ", status=" + status + "]";
	}

}
