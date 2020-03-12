package jheister.lmdbinternals;

enum PageType {
    P_BRANCH	 (0x01),
    P_LEAF		 (0x02),
    P_OVERFLOW	 (0x04),
    P_META		 (0x08),
    P_DIRTY		 (0x10),//todo: ?? other flag, not page
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
