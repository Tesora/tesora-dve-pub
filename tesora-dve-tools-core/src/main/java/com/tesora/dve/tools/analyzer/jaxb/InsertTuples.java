//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.2.4-2 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2013.11.01 at 06:28:31 PM EDT 
//


package com.tesora.dve.tools.analyzer.jaxb;

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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for InsertTuples complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="InsertTuples">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;attribute name="tupleCount" use="required" type="{http://www.w3.org/2001/XMLSchema}int" />
 *       &lt;attribute name="tuplePop" use="required" type="{http://www.w3.org/2001/XMLSchema}int" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "InsertTuples")
public class InsertTuples {

    @XmlAttribute(name = "tupleCount", required = true)
    protected int tupleCount;
    @XmlAttribute(name = "tuplePop", required = true)
    protected int tuplePop;

    /**
     * Gets the value of the tupleCount property.
     * 
     */
    public int getTupleCount() {
        return tupleCount;
    }

    /**
     * Sets the value of the tupleCount property.
     * 
     */
    public void setTupleCount(int value) {
        this.tupleCount = value;
    }

    /**
     * Gets the value of the tuplePop property.
     * 
     */
    public int getTuplePop() {
        return tuplePop;
    }

    /**
     * Sets the value of the tuplePop property.
     * 
     */
    public void setTuplePop(int value) {
        this.tuplePop = value;
    }

}