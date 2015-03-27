package com.deltavista.data.structures.cache.utils;

import com.deltavista.data.structures.cache.direct.DirectCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Created by t.sikora on 2015-03-17.
 */
class CacheConvertert
{
    public static void main(String[] args) throws IOException
    {
        ByteBuffer mainId = ByteBuffer.allocate(4);
        ByteBuffer size = ByteBuffer.allocate(2);
        ByteBuffer sizeInt = ByteBuffer.allocate(4);
        ByteBuffer record = ByteBuffer.allocate(1936582980);
        ByteBuffer longCombined = ByteBuffer.allocate(8);
        FileChannel fileChannel = FileChannel.open(Paths.get(args[0]));
        OpenOption[] wc = {StandardOpenOption.CREATE,StandardOpenOption.WRITE };

        FileChannel fileChannelMain = FileChannel.open(Paths.get(args[0]+"Main"),
           wc );
        FileChannel fileChannelInt = FileChannel.open(Paths.get(args[0]+"Int"),
            wc);
        FileChannel fileChannelLong = FileChannel.open(Paths.get(args[0]+"Long"),
            wc);
        long position = 0;
        long combined;
        int length;
        while(fileChannel.position()<fileChannel.size())
        {
            record.clear();
            mainId.clear();
            size.clear();
            sizeInt.clear();
            fileChannel.read(mainId);
            mainId.flip();
            fileChannelInt.write(mainId);
            fileChannel.read(size);
            size.flip();
            if((size.get(0) & (1 << 7)) != 0)
            {
                char mask = (char) 0x7fff;
                char d = (char) (size.getChar() & mask);
                sizeInt.position(2);
                sizeInt.putChar(d);
            }
            else
            {
                size.position(0);
                sizeInt.position(0);
                sizeInt.putChar(size.getChar());
                fileChannel.read(sizeInt);
            }
            sizeInt.position(0);
            length = sizeInt.getInt();
            record.limit(length);
            fileChannel.read(record);
            record.flip();
            fileChannelMain.write(record);
            combined = DirectCache.combine(length, position);
            position+=length;
            longCombined.clear();
            longCombined.putLong(combined);
            longCombined.flip();
            fileChannelLong.write(longCombined);

        }
    }
}
