package jheister.lmdbinternals;

import org.junit.Test;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;

public class AppTest extends TestBase {
    @Test
    public void shouldAnswerWithTrue() throws IOException {
        File file = new File(tmp.getRoot(), "lmdb");
        try (Env<ByteBuffer> env = Env.create().open(file, MDB_NOSUBDIR)) {
            Dbi<ByteBuffer> db = env.openDbi((byte[]) null);

            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                put(txn, db, "Hello", "World");
                txn.commit();
            }

            long pageCount = file.length() / 4096;
            System.out.println("DB file has " + pageCount + " pages.");
            ByteBuffer buffer = new RandomAccessFile(file, "r").getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length()).order(LITTLE_ENDIAN);


            for (int i = 0; i < pageCount; i++) {
                Page page = new Page(buffer, i * 4096);

                System.out.println(page.pageNumber() + " " + page.pageType());

                if(page.pageType() == PageType.P_META) {
                    System.out.println(page.meta);
                }
            }
        }
    }
}
