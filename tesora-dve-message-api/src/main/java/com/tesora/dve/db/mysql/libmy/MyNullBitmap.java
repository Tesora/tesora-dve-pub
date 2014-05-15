// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

import com.tesora.dve.exceptions.PEException;

public class MyNullBitmap {

	public enum BitmapType {RESULT_ROW, EXECUTE_REQUEST}

	private byte[] bitmap;
	private int numFields;
	private BitmapType bitmapType;
	
	public MyNullBitmap(int numFields, BitmapType bitmapType) {
		this.bitmapType = bitmapType;
		this.numFields = numFields;
		
		init();
	}

	public MyNullBitmap (byte[] bitmap, int numFields, BitmapType bitmapType) {
		this.bitmap = bitmap;
		this.bitmapType = bitmapType;
		this.numFields = numFields;
	}
	

	private void init() {
		int bitmapBytes = computeSize(numFields, bitmapType);
		bitmap = new byte[bitmapBytes];
		// init to zeroes
		for (int i = 0; i < bitmap.length; i++) {
			bitmap[i] = 0;
		}
	}
	
	public byte[] getBitmap() {
		return bitmap;
	}

	public static int computeSize(int numFields, BitmapType bitmapType2)  {
		return (numFields + 7 + getOffset(bitmapType2)) / 8;
	}

	public void setBit(int position) throws IllegalArgumentException {
		if ( position > numFields ) 
			throw new IllegalArgumentException("Cannot set value at position " + position + " for Null Bitmap containing only " + numFields + " elements");

		int positionIndex = position - 1;
		int offset = getOffset(bitmapType);
		int inByte = ((positionIndex + offset) / 8);
		int bitPos = (positionIndex + offset) % 8;
		bitmap[inByte] |= 1 << bitPos;
	}

	public boolean getBit(int position) throws IllegalArgumentException {
		if ( position > numFields ) 
			throw new IllegalArgumentException("Cannot return value at position " + position + " from Null Bitmap containing only " + numFields + " elements");
		int positionIndex = position - 1;
		int offset = getOffset(bitmapType);
		int inByte = ((positionIndex + offset) / 8);
		int bitPos = (positionIndex + offset) % 8;
		int mask = 1 << bitPos;

		return (( bitmap[inByte] & mask ) == mask);
	}
	
	public byte[] getBitmapArray() {
		return bitmap;
	}
	
	public int size() {
		return numFields;
	}
	
	public int length() {
		return bitmap.length;
	}
	
	private static int getOffset(BitmapType bitmapType2) {
		int offset = 0;
		switch (bitmapType2) {
		case EXECUTE_REQUEST:
			offset = 0;
			break;
		case RESULT_ROW:
			offset = 2;
			break;
		default :
			break;
		}
		return offset;
	}
	
	public MyNullBitmap flipType() throws PEException {
		MyNullBitmap newBitmap = 
				new MyNullBitmap(size(),
						(bitmapType == BitmapType.RESULT_ROW) ? BitmapType.EXECUTE_REQUEST : BitmapType.RESULT_ROW);
		if (bitmapType == BitmapType.EXECUTE_REQUEST){
			int oldByteIndex = 0;
			for (int iNewBitmap = 0; iNewBitmap < newBitmap.length(); ++iNewBitmap) {
				byte newByte = 0;
				if (oldByteIndex < bitmap.length)
					newByte |= (byte) (bitmap[oldByteIndex] << 2);
				if (oldByteIndex > 0)
					newByte |= (bitmap[oldByteIndex-1] >> 6 & 0x03);
				++oldByteIndex;
				newBitmap.bitmap[iNewBitmap] = newByte;
			}
		} else {
			int oldByteIndex = 0;
			for (int iNewBitmap = 0; iNewBitmap < newBitmap.length(); ++iNewBitmap) {
				byte oldByte = bitmap[oldByteIndex++];
				byte newByte = (byte) ((int)oldByte >> 2 & 0x3f);
				if (oldByteIndex < bitmap.length)
					newByte |= bitmap[oldByteIndex] << 6;
				newBitmap.bitmap[iNewBitmap] = newByte;
			}
		}
		return newBitmap;
	}
}
