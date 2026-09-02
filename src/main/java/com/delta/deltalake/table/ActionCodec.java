package com.delta.deltalake.table;

import com.delta.deltalake.log.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Locale;

final class ActionCodec {
    private ActionCodec() {}

    static LogAction decode(String type, Object action, ObjectMapper mapper) {
        return switch (type.toLowerCase(Locale.ROOT)) {
            case "add" -> mapper.convertValue(action, AddFile.class);
            case "remove" -> mapper.convertValue(action, RemoveFile.class);
            case "metadata" -> mapper.convertValue(action, Metadata.class);
            case "protocol" -> mapper.convertValue(action, Protocol.class);
            case "commitinfo" -> mapper.convertValue(action, CommitInfo.class);
            case "txn" -> mapper.convertValue(action, Txn.class);
            default -> throw new IllegalArgumentException("Unsupported log action: " + type);
        };
    }

    static LogRecord encode(LogAction action) {
        String type = switch (action) {
            case AddFile ignored -> "add";
            case RemoveFile ignored -> "remove";
            case Metadata ignored -> "metadata";
            case Protocol ignored -> "protocol";
            case CommitInfo ignored -> "commitInfo";
            case Txn ignored -> "txn";
        };
        return new LogRecord(type, action);
    }
}
