// OS_STATUS: public
package com.tesora.dve.db.mysql;

import com.tesora.dve.charset.NativeCharSet;
import com.tesora.dve.charset.mysql.MysqlNativeCharSetCatalog;
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
