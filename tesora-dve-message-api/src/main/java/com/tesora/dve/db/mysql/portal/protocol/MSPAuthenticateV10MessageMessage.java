package com.tesora.dve.db.mysql.portal.protocol;

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

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import org.apache.commons.codec.digest.DigestUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

public class MSPAuthenticateV10MessageMessage extends BaseMSPMessage<MSPAuthenticateV10MessageMessage.ParsedData> implements MSPUntypedMessage {
    private static final int MAX_PACKET_SIZE = 0xffffff;

    static class ParsedData {
        ClientCapabilities caps;
        int maxPacketSize;
        byte charsetID;
        String username;
        String password;
        String initialDatabase;
    }

    public MSPAuthenticateV10MessageMessage() {
        super();
    }

    public MSPAuthenticateV10MessageMessage(byte sequenceID, ByteBuf backing) {
        super(sequenceID,backing);
    }

    @Override
    public MSPAuthenticateV10MessageMessage newPrototype(byte sequenceID, ByteBuf source) {
        final byte messageType = source.readByte();
        source = source.slice();
        return new MSPAuthenticateV10MessageMessage(sequenceID,source);
    }

    @Override
    public byte getMysqlMessageType() {
        return (byte) 0xc0;
    }

    @Override
    protected ParsedData unmarshall(ByteBuf source) {
        ParsedData parseValues = new ParsedData();
        parseValues.caps = new ClientCapabilities(source.readUnsignedInt());
        parseValues.maxPacketSize = source.readInt();
        parseValues.charsetID = source.readByte();

        source.skipBytes(23); // login request has a 23 byte filler
        parseValues.username = source.readSlice(source.bytesBefore((byte) 0)).toString(CharsetUtil.UTF_8);
        source.skipBytes(1); // skip the NULL terminator
        byte passwordLength = source.readByte();
        parseValues.password = source.readSlice(passwordLength).toString(CharsetUtil.ISO_8859_1);

        // if the clientCapabilities flag has the CLIENT_CONNECT_WITH_DB bit set,
        // then this message contains an initial database to connect to
        if ( parseValues.caps.connectWithDB() ) {
            parseValues.initialDatabase = source.readSlice(source.bytesBefore((byte) 0)).toString(CharsetUtil.UTF_8);
            source.skipBytes(1); // skip the NULL terminator
        } else {
            parseValues.initialDatabase = "";
        }
        return parseValues;
    }

    public ClientCapabilities getClientCapabilities() {
        return readState().caps;
    }

    public int getMaxPacketSize() {
        return readState().maxPacketSize;
    }

    public byte getCharsetID() {
        return readState().charsetID;
    }

    public String getUsername() {
        return readState().username;
    }

    public String getPassword() {
        return readState().password;
    }

    public String getInitialDatabase() {
        return readState().initialDatabase;
    }

    public static void write(ByteBuf out, String userName, String userPassword, String salt, Charset charset, int mysqlCharsetID, int capabilitiesFlag) {
        ByteBuf leBuf = out.order(ByteOrder.LITTLE_ENDIAN);

        int payloadSizeIndex = leBuf.writerIndex();
        leBuf.writeMedium(0);
        leBuf.writeByte(1);
        int payloadStartIndex = leBuf.writerIndex();
        leBuf.writeInt(capabilitiesFlag);
        leBuf.writeInt(MAX_PACKET_SIZE);
//		leBuf.writeByte(serverGreeting.getServerCharsetId());
        leBuf.writeByte(mysqlCharsetID);
        leBuf.writeZero(23);
        leBuf.writeBytes(userName.getBytes(charset));
        leBuf.writeZero(1);


        if ((capabilitiesFlag & ClientCapabilities.CLIENT_SECURE_CONNECTION) > 0) {

            byte[] securePassword = computeSecurePassword(userPassword, salt);
            leBuf.writeByte(securePassword.length);
            leBuf.writeBytes(securePassword);
        } else {
            leBuf.writeBytes(userPassword.getBytes(charset));
            leBuf.writeZero(1);
        }

        leBuf.setMedium(payloadSizeIndex, leBuf.writerIndex()-payloadStartIndex);
    }

    public static byte[] computeSecurePassword(String password, String salt) {
        byte[] sha1password = DigestUtils.sha1(password);
        byte[] seedbytes = salt.getBytes();
        ByteBuffer bb = ByteBuffer.allocate(sha1password.length + seedbytes.length);
        bb.put(seedbytes);
        bb.put(DigestUtils.sha1(sha1password));
        byte[] sha1parttwo = DigestUtils.sha1(bb.array());
        byte[] securePassword = new byte[sha1password.length];
        for(int i = 0; i < securePassword.length; ++i)
            securePassword[i] = (byte) (sha1password[i] ^ sha1parttwo[i]);
        return securePassword;
    }

    public static String computeSecurePasswordString(String password, String salt) {
        return new String(computeSecurePassword(password, salt), CharsetUtil.ISO_8859_1);
    }


}
