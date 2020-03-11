package jheister.lmdbinternals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.Txn;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;

public class AppTest {
    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void shouldAnswerWithTrue() throws IOException {
        File file = new File(tmp.getRoot(), "lmdb");
        try (Env<ByteBuffer> env = Env.create().open(file, EnvFlags.MDB_NOSUBDIR)) {
            Dbi<ByteBuffer> db = env.openDbi((byte[]) null);

            writeSomeStuff(env, db);
            writeSomeStuff(env, db);


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

    private void writeSomeStuff(Env<ByteBuffer> env, Dbi<ByteBuffer> db) {
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            ByteBuffer keyBuffer = ByteBuffer.allocateDirect(12);
            ByteBuffer valueBuffer = ByteBuffer.allocateDirect(12);

            keyBuffer.put("Hello".getBytes(UTF_8));
            valueBuffer.put("World".getBytes(UTF_8));
            keyBuffer.flip();
            valueBuffer.flip();

            db.put(txn, keyBuffer, valueBuffer);
            txn.commit();
        }
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
