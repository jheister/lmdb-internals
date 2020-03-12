package jheister.lmdbinternals;

import org.junit.Test;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;

public class MainDbRead extends TestBase {
    @Test public void
    can_read_values_in_order_from_leaf_page() throws IOException {
        File file = new File(tmp.getRoot(), "lmdb");
        try (Env<ByteBuffer> env = Env.create().open(file, MDB_NOSUBDIR)) {
            Dbi<ByteBuffer> db = env.openDbi((byte[]) null);

            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                put(txn, db, "Hello", "World");
                put(txn, db, "Goodbye", "Others");
                txn.commit();
            }
        }

        LmdbDataFile lmdbDataFile = new LmdbDataFile(file);

        lmdbDataFile.mainRoot().forEachNode(node -> {
            System.out.println(new String(node.key(), UTF_8) + ":" + new String(node.value(), UTF_8));
        });
    }

    @Test public void when_node_shrinks_other_data_in_page_is_moved_to_remove_fragmentation() throws Exception {
        File file = new File(tmp.getRoot(), "lmdb");
        try (Env<ByteBuffer> env = Env.create().open(file, MDB_NOSUBDIR)) {
            Dbi<ByteBuffer> db = env.openDbi((byte[]) null);

            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                put(txn, db, "A", "abc");
                put(txn, db, "B", "def");
                put(txn, db, "C", "ghi");
                txn.commit();
            }

            try (LmdbDataFile lmdbDataFile = new LmdbDataFile(file)) {
                Page root = lmdbDataFile.mainRoot();
                for (int i = 0; i < root.numEntries(); i++) {
                    Node node = root.getNode(i);
                    System.out.println(new String(node.key(), UTF_8) + " stored at " + (node.offset - root.offset));
                }
            }

            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                put(txn, db, "A", "a");
                put(txn, db, "B", "def");
                put(txn, db, "C", "ghi");
                txn.commit();
            }

            System.out.println("Afterwards");

            try (LmdbDataFile lmdbDataFile = new LmdbDataFile(file)) {
                Page root = lmdbDataFile.mainRoot();
                for (int i = 0; i < root.numEntries(); i++) {
                    Node node = root.getNode(i);
                    System.out.println(new String(node.key(), UTF_8) + " stored at " + (node.offset - root.offset));
                }
            }
        }
    }

    @Test public void
    read_in_order_across_several_pages() throws IOException {
        File file = new File(tmp.getRoot(), "lmdb");
        try (Env<ByteBuffer> env = Env.create().open(file, MDB_NOSUBDIR)) {
            Dbi<ByteBuffer> db = env.openDbi((byte[]) null);

            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                for (int i = 0; i < 1000; i++) {
                    put(txn, db, "key-" + i, UUID.randomUUID().toString());
                }
                txn.commit();
            }
        }

        LmdbDataFile lmdbDataFile = new LmdbDataFile(file);

        System.out.println(lmdbDataFile.mainRoot());


        forEachEntry(lmdbDataFile, lmdbDataFile.mainRoot(), node -> {
            System.out.println(new String(node.key(), UTF_8) + ":" + new String(node.value(), UTF_8));
        });
    }

    private void forEachEntry(LmdbDataFile file, Page page, Consumer<Node> consumer) {
        if (page.pageType() == PageType.P_LEAF) {
            page.forEachNode(consumer);
        } else if (page.pageType() == PageType.P_BRANCH) {
            page.forEachNode(node -> {
                forEachEntry(file, file.getPage(node.childPage()), consumer);
            });
        }
    }
}
