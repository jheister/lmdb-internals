package jheister.lmdbinternals;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class TestBase {
    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(12);
    private final ByteBuffer valueBuffer = ByteBuffer.allocateDirect(36);

    public void put(Txn<ByteBuffer> txn, Dbi<ByteBuffer> db, String key, String value) {
        keyBuffer.clear();
        valueBuffer.clear();
        keyBuffer.put(key.getBytes(UTF_8));
        valueBuffer.put(value.getBytes(UTF_8));
        keyBuffer.flip();
        valueBuffer.flip();

        db.put(txn, keyBuffer, valueBuffer);
    }
}
