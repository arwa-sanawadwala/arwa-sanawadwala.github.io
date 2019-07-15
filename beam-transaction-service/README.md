# Transaction service which will generate stream of data and use stateful processing

* Entry point is TransactionService
* TransactionService holds logic for:
    * Generation of stream data.
    * Read DB data from files.
    * Process both the streams generated simultaneously and extract mapped records.
    * Apply logic on mapped records along with db data read from files.
    * Write final output in files which is sharded according to window.

