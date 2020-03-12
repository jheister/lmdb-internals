package jheister.lmdbinternals;

import java.nio.ByteBuffer;

public class Node {
    private final ByteBuffer buffer;
    public final int offset;

    private static final int DATA_SIZE = 0;
    private static final int FLAGS = 4;
    private static final int KEY_SIZE = 6;
    private static final int KEY_DATA = 8;

    public Node(ByteBuffer buffer, int offset) {
        this.buffer = buffer;
        this.offset = offset;
    }

    public byte[] key() {
        byte[] key = new byte[keySize()];
        getBytes(key, offset + KEY_DATA);
        return key;
    }

    public byte[] value() {
        int size = valueSize();
        byte[] value = new byte[size];
        getBytes(value, offset + KEY_DATA + keySize());
        return value;
    }

    private int keySize() {
        return Short.toUnsignedInt(buffer.getShort(offset + KEY_SIZE));
    }

    private int valueSize() {
        return Short.toUnsignedInt(buffer.getShort(offset + DATA_SIZE));
    }

    public int childPage() {
        return valueSize();
    }

    private void getBytes(byte[] value, int newPosition) {
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position(newPosition);
        duplicate.get(value);
    }
}
