package jheister.lmdbinternals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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

    public Page subPage() {
        return new Page(buffer, offset + KEY_DATA + keySize());//todo: limt size somehow?
    }

    private int keySize() {
        return Short.toUnsignedInt(buffer.getShort(offset + KEY_SIZE));
    }

    private int valueSize() {
        return Short.toUnsignedInt(buffer.getShort(offset + DATA_SIZE));
    }

    public List<Flag> flags() {
        return Flag.from(buffer.getShort(offset + FLAGS));
    }

    public boolean is(Flag flag) {
        return (buffer.getShort(offset + FLAGS) & flag.value) != 0;
    }

    public int childPage() {
        return valueSize();//todo: should be long including flags since 64bit
    }

    private void getBytes(byte[] value, int newPosition) {
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position(newPosition);
        duplicate.get(value);
    }

    public Db subDb() {
        return new Db(buffer, offset + KEY_DATA + keySize());
    }

    public enum Flag {
        F_BIGDATA(0x01),
        F_SUBDATA(0x02),
        F_DUPDATA(0x04);

        private int value;

        Flag(int value) {
            this.value = value;
        }

        public static List<Flag> from(short flags) {
            List<Flag> result = new ArrayList<>();
            for (Flag flag : Flag.values()) {
                if ((flags & flag.value) != 0) {
                    result.add(flag);
                }
            }
            return result;
        }
    }
}
