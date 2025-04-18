/***************************************************************************
 *   Copyright (C) 2008 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   This software may be modified and distributed under the terms         *
 *   of the MIT license.  See the LICENSE file for details.                *
 *                                                                         *
 ***************************************************************************/

#ifndef WTF
#define WTF


#include <ctime>
#include <string>

// Fix for PRId64 (See https://stackoverflow.com/a/18719205)
#if defined(__cplusplus) && !defined(__STDC_FORMAT_MACRO)
#define __STDC_FORMAT_MACRO 1 // Not sure where to put this
#endif 
#include <inttypes.h>


#define ZKY_LOG_TIME_FORMAT "%Y-%m-%d %H:%M:%S"
#define ZKY_OUTPUT_STREAM stdout

#define __SHORT_FILE__                            \
  ({                                              \
    constexpr cstr sf__{PastLastSlash(__FILE__)}; \
    sf__;                                         \
  })


#define ZKY_TRACE(...)                                                        \
  outputLogHeade_(__SHORT_FILE__, __LINE__, __FUNCTION__, 0); \
  ::fprintf(ZKY_OUTPUT_STREAM, __VA_ARGS__);                                  \
  fprintf(ZKY_OUTPUT_STREAM, "\n");                                           \
  ::fflush(stdout)


// Output log message header in this format: [type] [file:line:function] time -
// ex: [ERROR] [somefile.cpp:123:doSome()] 2008/07/06 10:00:00 -
inline void outputLogHeade_(const char *file, int line, const char *func,
                             int level) {
  time_t t = ::time(NULL);
  tm *curTime = localtime(&t);
  char time_str[32];  // FIXME
  ::strftime(time_str, 32, ZKY_LOG_TIME_FORMAT, curTime);
  const char *type;
  type = "ZKY";

  // PAVLO: DO NOT CHANGE THIS
  ::fprintf(ZKY_OUTPUT_STREAM, "%s [%s:%d:%s] %s - ",
            time_str,
            file, line, func,
            type);
}


#endif
