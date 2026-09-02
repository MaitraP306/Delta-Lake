package com.delta.deltalake.log;

public record Protocol(int minReaderVersion, int minWriterVersion) implements LogAction {}
