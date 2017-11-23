package global;

import simpledb.TransactionId;

/**
 * Class holding global fields.
 */
public class Global {

    // For distributed, we only have one transaction per machine.
    public static final TransactionId TRANSACTION_ID = new TransactionId();

    // IP Address for localhost
    public static final String LOCALHOST = "127.0.0.1";

}