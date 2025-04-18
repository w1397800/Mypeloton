//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stack_trace.h
//
// Identification: src/include/common/stack_trace.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//




void RegisterSignalHandlers();

void PrintStackTrace(FILE *out = ::stderr,
                     unsigned int max_frames = 63);

void SignalHandler(int signum);


