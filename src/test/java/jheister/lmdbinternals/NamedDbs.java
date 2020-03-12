package jheister.lmdbinternals;

import org.junit.Test;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.io.File;
import java.nio.ByteBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;

public class NamedDbs extends TestBase {
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

    @Test public void
    named_db_has_root_pageNr_in_main_db() {
        File file = new File(tmp.getRoot(), "lmdb");
        try (Env<ByteBuffer> env = Env.create().open(file, MDB_NOSUBDIR)) {
            Dbi<ByteBuffer> db = env.openDbi("named-db", DbiFlags.MDB_CREATE);
            Dbi<ByteBuffer> mainDb = env.openDbi((byte[]) null);

            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                put(txn, db, "Hello", "World");
                txn.commit();
            }

            try (Txn<ByteBuffer> txn = env.txnRead()) {
                mainDb.iterate(txn).forEachRemaining(kv -> {
                    kv.val().order(LITTLE_ENDIAN);
                    System.out.println(string(kv.key()) + " " + new Db(kv.val(), 0));
                });
            }
        }
    }


    public static String string(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, UTF_8);
    }
}
