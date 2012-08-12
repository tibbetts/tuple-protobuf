#!/bin/sh
/usr/local/bin/protoc --proto_path=src --java_out=gen src/test_messages.proto
