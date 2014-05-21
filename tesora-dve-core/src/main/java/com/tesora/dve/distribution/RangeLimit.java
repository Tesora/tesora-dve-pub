// OS_STATUS: public
package com.tesora.dve.distribution;

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

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;

@XmlRootElement(namespace=XMLConstants.W3C_XML_SCHEMA_NS_URI) 
public class RangeLimit {
	
	@XmlElement(name="value",namespace=XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI)
	List<Object> objList = new LinkedList<Object>();

	public RangeLimit(KeyValue keyValue) {
		for(ColumnDatum cd : keyValue.values())
			this.add(cd.getValue());
	}

	public RangeLimit() {
	}

	public void add(Object objToAdd) {
		objList.add(objToAdd);
	}
	
	public int size() {
		return objList.size();
	}
	
	public Iterator<Object> iterator() {
		return objList.iterator();
	}
	
	@Override
	public String toString() {
		String result;
		try {
			JAXBContext jc = JAXBContext.newInstance(RangeLimit.class);
			Marshaller m = jc.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			StringWriter sw = new StringWriter();
			m.marshal(this, sw);
			result = sw.toString();
		} catch (JAXBException e) {
			throw new PECodingException("Unable to serialize RangeLimit",e);
		}
		return result;
	}
	
	public static RangeLimit parseLimit(String serializedRange) throws PEException {
		RangeLimit result;
		try {
			JAXBContext jc = JAXBContext.newInstance(RangeLimit.class);
			Unmarshaller u = jc.createUnmarshaller();
			StringReader sr = new StringReader(serializedRange);
			result = (RangeLimit) u.unmarshal(sr);
			
		} catch (JAXBException e) {
			throw new PEException("Cannot parse RangeLimit: " + serializedRange);
		}
		return result;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((objList == null) ? 0 : objList.hashCode());
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
		RangeLimit other = (RangeLimit) obj;
		if (objList == null) {
			if (other.objList != null)
				return false;
		} else if (!objList.equals(other.objList))
			return false;
		return true;
	}
}