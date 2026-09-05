package com.delta.deltalake.log;
public sealed interface LogAction
        permits AddFile, RemoveFile, Metadata, Protocol, CommitInfo, Txn {

    public class AddFile {
    }
}
