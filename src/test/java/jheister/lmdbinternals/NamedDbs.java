package jheister.lmdbinternals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.io.File;
import java.nio.ByteBuffer;

import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;

public class NamedDbs {
    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(12);
    private final ByteBuffer valueBuffer = ByteBuffer.allocateDirect(12);

    @Test
    public void performanceOfEmptyCommit() {
        for (int dbCount = 1; dbCount <= 2048; dbCount *= 2) {
            File file = new File(tmp.getRoot(), "lmdb");
            try (Env<ByteBuffer> env = Env.create().setMaxDbs(dbCount).open(file, MDB_NOSUBDIR)) {
                for (int i = 0; i < dbCount; i++) {
                    env.openDbi("db-" + i, MDB_CREATE);
                }

                long before = System.currentTimeMillis();
                for (int i = 0; i < 100000; i++) {
                    try (Txn<ByteBuffer> txn = env.txnWrite()) {
                        txn.commit();
                    }
                }
                long after = System.currentTimeMillis();
                System.out.println(dbCount + " it took " + (after - before));
            }
            file.delete();
        }
    }
}
