package exchange.core2.cluster.model;

public enum ExchangeCommandCode {

    PLACE_ORDER((byte) 1, true),
    CANCEL_ORDER((byte) 2, true),
    MOVE_ORDER((byte) 3, true),
    REDUCE_ORDER((byte) 4, true),
    ORDER_BOOK_REQUEST((byte) 5, false),

    ADD_CLIENT((byte) 10, true),
    BALANCE_ADJUSTMENT((byte) 11, true),
    SUSPEND_CLIENT((byte) 12, true),
    RESUME_CLIENT((byte) 13, true),

    BINARY_DATA_QUERY((byte) 90, false),
    BINARY_DATA_COMMAND((byte) 91, true),

    NOP((byte) 120, false),
    RESET((byte) 124, true);


    private final byte code;
    private final boolean mutate;

    ExchangeCommandCode(byte code, boolean mutate) {
        this.code = code;
        this.mutate = mutate;
    }

    public byte getCode() {
        return code;
    }

    public boolean isMutate() {
        return mutate;
    }

    public static boolean isOrderBookRelated(final byte code) {
        return code < 10;
    }

    public static ExchangeCommandCode fromCode(final byte code) {
        switch (code) {
            case 1:
                return PLACE_ORDER;
            case 2:
                return CANCEL_ORDER;
            case 3:
                return MOVE_ORDER;
            case 4:
                return REDUCE_ORDER;
            case 5:
                return ORDER_BOOK_REQUEST;
            case 10:
                return ADD_CLIENT;
            case 11:
                return BALANCE_ADJUSTMENT;
            case 12:
                return SUSPEND_CLIENT;
            case 13:
                return RESUME_CLIENT;
            case 90:
                return BINARY_DATA_QUERY;
            case 91:
                return BINARY_DATA_COMMAND;
            case 120:
                return NOP;
            case 124:
                return RESET;
            default:
                throw new RuntimeException("Unknown ExchangeCommandCode: " + code);
        }

    }

}
