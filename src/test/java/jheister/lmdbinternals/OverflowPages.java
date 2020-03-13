package jheister.lmdbinternals;

import org.junit.Test;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertTrue;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;

public class OverflowPages extends TestBase {
    @Test public void
    look_at_overflow_pages() throws IOException {
        String largeData = IntStream.range(0, 4096).mapToObj(i -> "A").collect(joining());

        File file = new File(tmp.getRoot(), "lmdb");
        try (Env<ByteBuffer> env = Env.create().open(file, MDB_NOSUBDIR)) {
            Dbi<ByteBuffer> dbi = env.openDbi((byte[]) null);

            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                put(txn, dbi, "key", largeData);
                txn.commit();
            }
        }

        LmdbDataFile lmdbDataFile = new LmdbDataFile(file);
        lmdbDataFile.mainRoot().forEachNode(n -> {
            System.out.println(new String(n.key(), UTF_8));
            String x = new String(n.value(), UTF_8);
            System.out.println(x);
            assertTrue(x.equals(largeData));
        });
    }
}
