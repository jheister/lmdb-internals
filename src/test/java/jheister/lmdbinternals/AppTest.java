package jheister.lmdbinternals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;

public class AppTest {
    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(12);
    private final ByteBuffer valueBuffer = ByteBuffer.allocateDirect(12);

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

            /*
            * LMDB has 2 DBs:
            *   free pages
            *   main DB
            *
            * when using named DBs:
            *   main DB stores name -> DB metadata (inc root)
            *   when read?
            *   when written?
            *     on commit we have to iterate through all DBs and write their root pages into the main DB
            *   consequence of setting large number of DBs?
            *     larger txn object
            *     iterations over all to init them
            * */

            try (Txn<ByteBuffer> txn = env.txnRead()) {
                mainDb.iterate(txn).forEachRemaining(kv -> {
                    kv.val().order(LITTLE_ENDIAN);
                    System.out.println(string(kv.key()) + " " + Db.read(kv.val()));
                });
            }
        }
    }

    //todo: test how ops get slower when you have many named DBs (begin txn, put, get, commit)

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
                buffer.position(i * 4096);
                long pageNo = buffer.getLong();
                int dupFixedSize = Short.toUnsignedInt(buffer.getShort());
                short flags = buffer.getShort();

                PageType type = PageType.from(flags);
                System.out.println("Page " + pageNo + " has type " + type);
                if (type == PageType.P_META) {
                    buffer.getInt();//skip free space/overflow space

                    Meta meta = Meta.read(buffer);
                    System.out.println(meta);
                }
            }
        }
    }

    private void put(Txn<ByteBuffer> txn, Dbi<ByteBuffer> db, String key, String value) {
        keyBuffer.clear();
        valueBuffer.clear();
        keyBuffer.put(key.getBytes(UTF_8));
        valueBuffer.put(value.getBytes(UTF_8));
        keyBuffer.flip();
        valueBuffer.flip();

        db.put(txn, keyBuffer, valueBuffer);
    }

    private static String string(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, UTF_8);
    }

    private static class Meta {
        private final Db freeSpaceDb;
        private final Db mainDb;
        private final long lastPage;
        private final long txnId;

        public Meta(Db freeSpaceDb, Db mainDb, long lastPage, long txnId) {
            this.freeSpaceDb = freeSpaceDb;
            this.mainDb = mainDb;
            this.lastPage = lastPage;
            this.txnId = txnId;
        }

        public static Meta read(ByteBuffer buffer) {
            int magic = buffer.getInt();
            int version = buffer.getInt();
            long fixedMapAddress = buffer.getLong();
            long mapSize = buffer.getLong();
            Db freeSpaceDb = Db.read(buffer);
            Db mainDb = Db.read(buffer);
            long lastPage = buffer.getLong();
            long txnId = buffer.getLong();
            return new Meta(freeSpaceDb, mainDb, lastPage, txnId);
        }

        @Override
        public String toString() {
            return "Meta{" +
                    "freeSpaceDb=" + freeSpaceDb +
                    ", mainDb=" + mainDb +
                    ", lastPage=" + lastPage +
                    ", txnId=" + txnId +
                    '}';
        }
    }

    private static class Db {
        private final int depth;
        private final long branchPages;
        private final long leafPages;
        private final long overflowPages;
        private final long entries;
        private final long rootPage;

        public Db(int depth, long branchPages, long leafPages, long overflowPages, long entries, long rootPage) {
            this.depth = depth;
            this.branchPages = branchPages;
            this.leafPages = leafPages;
            this.overflowPages = overflowPages;
            this.entries = entries;
            this.rootPage = rootPage;
        }

        public static Db read(ByteBuffer buffer) {
            long pad = Integer.toUnsignedLong(buffer.getInt());
            short flags = buffer.getShort();
            int depth = Short.toUnsignedInt(buffer.getShort());
            long branchPages = buffer.getLong();
            long leafPages = buffer.getLong();
            long overflowPages = buffer.getLong();
            long entries = buffer.getLong();
            long rootPage = buffer.getLong();

            return new Db(depth, branchPages, leafPages, overflowPages, entries, rootPage);
        }

        @Override
        public String toString() {
            return "Db{" +
                    "depth=" + depth +
                    ", branchPages=" + branchPages +
                    ", leafPages=" + leafPages +
                    ", overflowPages=" + overflowPages +
                    ", entries=" + entries +
                    ", rootPage=" + rootPage +
                    '}';
        }
    }

    private enum PageType {
        P_BRANCH	 (0x01),
        P_LEAF		 (0x02),
        P_OVERFLOW	 (0x04),
        P_META		 (0x08),
        P_DIRTY		 (0x10),
        P_LEAF2		 (0x20),
        P_SUBP		 (0x40);

        private final int value;

        PageType(int value) {
            this.value = value;
        }

        public static PageType from(short flags) {
            for (PageType type : PageType.values()) {
                if ((flags & type.value) != 0) {
                    return type;
                }
            }
            return null;
        }
    }
}
