package com.tesora.dve.db.mysql;

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

import com.tesora.dve.charset.NativeCharSet;
import com.tesora.dve.charset.MysqlNativeCharSetCatalog;
import com.tesora.dve.db.mysql.portal.protocol.MyBackendDecoder;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;

public class CharsetDecodeHelper implements MyBackendDecoder.CharsetDecodeHelper {

    @Override
    public boolean typeSupported(MyFieldType fieldType, short flags, int maxDataLen) {
        boolean supported;MysqlNativeType mnt;
        try {
            mnt = ((MysqlNative) Singletons.require(HostService.class).getDBNative()).getNativeTypeFromMyFieldType(fieldType, flags, maxDataLen);
        } catch (PEException e) {
            throw new PECodingException("Couldn't lookup native type", e);
        }
        supported = (mnt != null);
        return supported;
    }

    @Override
    public long lookupMaxLength(byte charSet) {
        long maxCharLength;NativeCharSet cs = MysqlNativeCharSetCatalog.DEFAULT_CATALOG.findNativeCharsetById(charSet);
        if (cs == null)
            throw new PECodingException("Column definition gets unsupported character set (" + charSet + ")");
        maxCharLength = cs.getMaxLen();
        return maxCharLength;
    }
}
