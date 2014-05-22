package com.tesora.dve.common;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Marshaller;
import javax.xml.bind.UnmarshalException;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.log4j.Logger;
import org.xml.sax.SAXParseException;

import com.tesora.dve.exceptions.PEException;

public class PEXmlUtils extends PEFileUtils {

	private static final Logger logger = Logger.getLogger(PEXmlUtils.class);

	public static String marshalJAXB(final Object element) throws PEException {
		StringWriter writer = new StringWriter();
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(new Class[] { element.getClass() });
			Marshaller marshaller = jaxbContext.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			marshaller.marshal(element, writer);
		} catch (Exception e) {
			throw new PEException("Failed to marshal xml - " + e.getMessage(), e);
		}
		return writer.toString();
	}

	public static <T> T unmarshalJAXB(final File source, final Class<T> cls) throws PEException {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(source));

			return (T) unmarshalJAXB(reader, cls, null);
		} catch (final FileNotFoundException e) {
			throw new PEException("Failed to unmarshal xml file '" + source.getAbsolutePath() + "' - " + e.getMessage(), e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (final IOException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
	}

	public static <T> T unmarshalJAXB(final InputStream is, final Class<T> cls) throws PEException {
		return unmarshalJAXB(new BufferedReader(new InputStreamReader(is)), cls, null);
	}

	public static <T> T unmarshalJAXB(final Reader reader, final Class<T> cls) throws PEException {
		return unmarshalJAXB(reader, cls, null);
	}

	@SuppressWarnings("unchecked")
	public static <T> T unmarshalJAXB(final Reader reader, final Class<T> cls, URL schemaUrl) throws PEException {
		try {
			JAXBContext jc = JAXBContext.newInstance(cls.getPackage().getName());
			Unmarshaller u = jc.createUnmarshaller();

			if (schemaUrl != null) {
				SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
				Schema schema = schemaFactory.newSchema(schemaUrl);

				u.setSchema(schema);
			}

			Object obj = u.unmarshal(reader);

			if (obj instanceof JAXBElement)
				obj = (T) ((JAXBElement<?>) obj).getValue();

			return (T) obj;
		} catch (UnmarshalException e) {
			Throwable linked = ((UnmarshalException) e).getLinkedException();

			if (linked instanceof SAXParseException) {
				SAXParseException spe = (SAXParseException) linked;
				throw new PEException("Failed to unmarshal xml - line:" + spe.getLineNumber() + " column:"
						+ spe.getColumnNumber() + " " + spe.getMessage(), spe);
			}
			throw new PEException("Failed to unmarshal xml - " + linked.getMessage(), linked);
		} catch (Exception e) {
			throw new PEException("Failed to unmarshal xml - " + e.getMessage(), e);
		}
	}

	public static <T> T unmarshalJAXB(String def, Class<T> cls) throws PEException {
		return unmarshalJAXB(def, cls, null);
	}

	public static <T> T unmarshalJAXB(String def, Class<T> cls, URL schema) throws PEException {
		return unmarshalJAXB(new StringReader(def), cls, schema);
	}
}
