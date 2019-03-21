/*
 * Copyright 2019 IIT Software GmbH
 *
 * IIT Software GmbH licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.swiftmq.impl.store.standard.index;

class Util
{
	static int readInt(byte[] b, int offset)
	{
		int pos = offset;
		int i1 = b[pos++] & 0xff;
		int i2 = b[pos++] & 0xff;
		int i3 = b[pos++] & 0xff;
		int i4 = b[pos++] & 0xff;
		int i = (i1 << 24) + (i2 << 16) + (i3 << 8) + (i4 << 0);
		return i;
	}
	
	static void writeInt(int i, byte[] b, int offset)
	{
		int pos = offset;
		b[pos++] = (byte)((i >>> 24) & 0xFF);
		b[pos++] = (byte)((i >>> 16) & 0xFF);
		b[pos++] = (byte)((i >>>  8) & 0xFF);
		b[pos++] = (byte)((i >>>  0) & 0xFF);
	}
	
	static long readLong(byte[] b, int offset)
	{
		long l = ((long)(readInt(b,offset)) << 32) + (readInt(b,offset+4) & 0xFFFFFFFFL);
		return l;
	}
	
	static void writeLong(long l, byte[] b, int offset)
	{
		int pos = offset;
		b[pos++] = (byte)((l >>> 56) & 0xFF);
		b[pos++] = (byte)((l >>> 48) & 0xFF);
		b[pos++] = (byte)((l >>> 40) & 0xFF);
		b[pos++] = (byte)((l >>> 32) & 0xFF);
		b[pos++] = (byte)((l >>> 24) & 0xFF);
		b[pos++] = (byte)((l >>> 16) & 0xFF);
		b[pos++] = (byte)((l >>>  8) & 0xFF);
		b[pos++] = (byte)((l >>>  0) & 0xFF);
	}
}

