package com.raftkv.log;

class InMemoryRaftLogTest extends RaftLogContractTest {

    @Override
    protected RaftLog newLog() {
        return new InMemoryRaftLog();
    }
}
