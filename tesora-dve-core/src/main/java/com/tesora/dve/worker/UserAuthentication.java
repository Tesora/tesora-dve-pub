// OS_STATUS: public
package com.tesora.dve.worker;


public class UserAuthentication {

	String userid;
	String password;
	boolean adminUser;
	
	public UserAuthentication(String userid, String password, boolean adminUser) {
		this.userid = userid;
		this.password = password;
		this.adminUser = adminUser;
	}

	public String getUserid() {
		return userid;
	}

	public String getPassword() {
		return password;
	}
	
	public boolean isAdminUser() {
		return adminUser;
	}
	
	@Override
	public String toString() {
		return new StringBuffer().append("UserAuth(").append(userid).append(adminUser ? "[admin]" : "").append(")").toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((password == null) ? 0 : password.hashCode());
		result = prime * result + ((userid == null) ? 0 : userid.hashCode());
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
		UserAuthentication other = (UserAuthentication) obj;
		if (password == null) {
			if (other.password != null)
				return false;
		} else if (!password.equals(other.password))
			return false;
		if (userid == null) {
			if (other.userid != null)
				return false;
		} else if (!userid.equals(other.userid))
			return false;
		return true;
	}
	
	
}
