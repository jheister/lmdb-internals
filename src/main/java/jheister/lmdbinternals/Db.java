package jheister.lmdbinternals;

import java.nio.ByteBuffer;

public class Db {
    private ByteBuffer buffer;
    private final int offset;

    public Db(ByteBuffer buffer, int offset) {
        this.buffer = buffer;
        this.offset = offset;
    }

    /*
     * pad: 4
     * flags: 2
     * */

    public int depth() {
        return Short.toUnsignedInt(buffer.getShort(offset + 6));
    }

    public long branchPages() {
        return buffer.getLong(offset + 8);
    }

    public long leafPages() {
        return buffer.getLong(offset + 16);
    }

    public long overflowPages() {
        return buffer.getLong(offset + 24);
    }

    public long entries() {
        return buffer.getLong(offset + 32);
    }

    public long rootPage() {
        return buffer.getLong(offset + 40);
    }

    @Override
    public String toString() {
        return "Db{" +
                "depth=" + depth() +
                ", branchPages=" + branchPages() +
                ", leafPages=" + leafPages() +
                ", overflowPages=" + overflowPages() +
                ", entries=" + entries() +
                ", rootPage=" + rootPage() +
                '}';
    }
}
