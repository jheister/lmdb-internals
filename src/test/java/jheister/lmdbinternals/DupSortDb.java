package jheister.lmdbinternals;

import org.junit.Test;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static jheister.lmdbinternals.Node.Flag.F_DUPDATA;
import static jheister.lmdbinternals.Node.Flag.F_SUBDATA;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;

public class DupSortDb extends TestBase {
    @Test
    public void analyze_dup_sort() throws IOException {
        File file = new File(tmp.getRoot(), "lmdb");
        try (Env<ByteBuffer> env = Env.create().open(file, MDB_NOSUBDIR)) {
            Dbi<ByteBuffer> db = env.openDbi((byte[]) null, DbiFlags.MDB_DUPSORT);

            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                put(txn, db, "Hello", "World");
                put(txn, db, "Hello", "Others");
                put(txn, db, "Bye", "World");
                put(txn, db, "Bye", "Others");
                put(txn, db, "Single", "Value");//todo: can I insert a value too large here and have it blow up when it becomes a subpage?

                for (int i = 0; i < 100; i++) {
                    put(txn, db, "Many", UUID.randomUUID().toString());
                }

                txn.commit();
            }
        }

        //A node with #F_DUPDATA but no #F_SUBDATA contains a subpage
        //this means the data of the key is a P_SUBP page (variable size?)

        LmdbDataFile lmdbDataFile = new LmdbDataFile(file);

        for (int i = 0; i <= lmdbDataFile.currentMetaPage().meta.lastPage(); i++) {
            System.out.println(lmdbDataFile.getPage(i));
        }

        lmdbDataFile.mainRoot().forEachNode(node -> {
            System.out.println(new String(node.key(), UTF_8));


            if (node.is(F_DUPDATA) && !node.is(F_SUBDATA)) {
                node.subPage().forEachNode(innerNode -> {
                    System.out.println("\t" + new String(innerNode.key(), UTF_8) + ":" + new String(innerNode.value(), UTF_8));
                });
            } else if (node.is(F_SUBDATA)) {
                System.out.println(node.subDb());
                System.out.println(lmdbDataFile.getPage(node.subDb().rootPage()));
            } else {
                System.out.println("\t:" + new String(node.value(), UTF_8));
            }
        });
    }
}
