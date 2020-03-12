package jheister.lmdbinternals;

import java.nio.ByteBuffer;

public class Meta {
    private ByteBuffer buffer;
    private final int offset;
    public final Db freeSpaceDb;
    public final Db mainDb;

    public Meta(ByteBuffer buffer, int offset) {
        this.buffer = buffer;
        this.offset = offset;
        freeSpaceDb = new Db(buffer, offset + 24);
        mainDb = new Db(buffer, offset + 24 + 48);
    }

    /*
     * magic:4
     * version:4
     * fixedMapAddr:8
     * mapSize:8
     * */

    public long lastPage() {
        return buffer.getLong(offset + 120);
    }

    public long txnId() {
        return buffer.getLong(offset + 128);
    }

    @Override
    public String toString() {
        return "Meta{" +
                "freeSpaceDb=" + freeSpaceDb +
                ", mainDb=" + mainDb +
                ", lastPage=" + lastPage() +
                ", txnId=" + txnId() +
                '}';
    }
}
