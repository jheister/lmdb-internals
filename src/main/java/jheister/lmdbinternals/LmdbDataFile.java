package jheister.lmdbinternals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static jheister.lmdbinternals.Page.PAGESIZE;

class LmdbDataFile implements AutoCloseable {
    private final ByteBuffer buffer;
    private final RandomAccessFile file;

    LmdbDataFile(File file) throws IOException {
        this.file = new RandomAccessFile(file, "r");
        buffer = this.file.getChannel()
                .map(FileChannel.MapMode.READ_ONLY, 0, file.length()).order(LITTLE_ENDIAN);
    }

    @Override
    public void close() throws Exception {
        file.close();
    }

    public Page currentMetaPage() {
        Page meta0 = new Page(buffer, 0);
        Page meta1 = new Page(buffer, PAGESIZE);

        if (meta1.meta.txnId() > meta0.meta.txnId()) {
            return meta1;
        } else {
            return meta0;
        }
    }

    public Page getPage(long nr) {
        return new Page(buffer, (int) (nr * PAGESIZE));
    }

    public Page mainRoot() {
        return getPage(currentMetaPage().meta.mainDb.rootPage());
    }
}
