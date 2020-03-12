package jheister.lmdbinternals;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class Page {
    public static final int PAGESIZE = 4096;

    public int offset;
    private final ByteBuffer buffer;
    public final Meta meta;

    public Page(ByteBuffer buffer, int offset) {
        this.buffer = buffer;
        meta = new Meta(buffer, offset + 16);
        this.offset = offset;
    }

    public long pageNumber() {
        return buffer.getLong(offset);
    }

    public int dupFixedSize() {
        return Short.toUnsignedInt(buffer.getShort(offset + 8));
    }

    public int numEntries() {
        int mpLower = Short.toUnsignedInt(buffer.getShort(offset + 12));

        return (mpLower - 16) >> 1;//todo: the shift accunts for the size of pointers?
    }

    public PageType pageType() {//todo: cannot get type, have to ask if it has type since can have several
        short flags = buffer.getShort(offset + 10);

        return PageType.from(flags);
    }

    public Node getNode(int index) {
        if (index >= numEntries()) {
            throw new NoSuchElementException("" + index);
        }

        return new Node(buffer, offset + nodePointerFor(index));
    }

    private int nodePointerFor(int index) {
        return Short.toUnsignedInt(buffer.getShort(offset + 16 + (index << 1)));
    }

    @Override
    public String toString() {
        return "Page{" +
                "nr=" + pageNumber() +
                ", type=" + pageType() +
                '}';
    }

    public void forEachNode(Consumer<Node> consumer) {
        for (int i = 0; i < numEntries(); i++) {
            consumer.accept(getNode(i));
        }
    }
}
