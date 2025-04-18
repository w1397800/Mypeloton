#pragma once

#include <cstdio>
#include <ctime>
#include <cstdarg>
#include <cassert>

#define LOG_OUTPUT_STREAM stdout


#define LOG_LOG_TIME_FORMAT "%Y-%m-%d %H:%M:%S"
#define L_LEVEL_ERROR 0
#define L_LEVEL_WARN 1
#define L_LEVEL_INFO 2

inline void LogHeader(const char *file, int line, const char *func, int level) {
  time_t t = ::time(nullptr);
  tm *curTime = localtime(&t);
  char time_str[32];
  ::strftime(time_str, 32, LOG_LOG_TIME_FORMAT, curTime);
  const char *type;
  switch (level) {
    case L_LEVEL_ERROR:
      type = "ERROR";
      break;
    case L_LEVEL_WARN:
      type = "WARN ";
      break;
    case L_LEVEL_INFO:
      type = "INFO ";
      break;
    default:
      type = "UNKWN";
  }
  ::fprintf(LOG_OUTPUT_STREAM, "%s [%s:%d:%s] %s - ", time_str, file, line, func, type);
}

#define L_INFO(format, ...) do { \
  LogHeader(__FILE__, __LINE__, __func__, L_LEVEL_INFO); \
  if (LOG_OUTPUT_STREAM != nullptr) {\
    ::fprintf(LOG_OUTPUT_STREAM, format, ##__VA_ARGS__); \
    ::fprintf(LOG_OUTPUT_STREAM, "\n");\
  }\
} while (false)

#define L_WARN(format, ...) do { \
  LogHeader(__FILE__, __LINE__, __func__, L_LEVEL_WARN); \
  if (LOG_OUTPUT_STREAM != nullptr) {\
    ::fprintf(LOG_OUTPUT_STREAM, format, ##__VA_ARGS__); \
    ::fprintf(LOG_OUTPUT_STREAM, "\n");\
  }\
} while (false)

#define L_ERROR(format, ...) do { \
  LogHeader(__FILE__, __LINE__, __func__, L_LEVEL_ERROR); \
  if (LOG_OUTPUT_STREAM != nullptr) {\
    ::fprintf(LOG_OUTPUT_STREAM, format, ##__VA_ARGS__); \
    ::fprintf(LOG_OUTPUT_STREAM, "\n");\
  }\
  assert(false); \
} while (false)

