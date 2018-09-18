#ifndef _CONFIG_H
#define _CONFIG_H

#ifdef __APPLE__
#include <AvailabilityMacros.h>
#endif

#ifdef __APPLE__
#include <malloc/malloc.h>
#define HAVE_MALLOC_SIZE 1
#define ledis_malloc_size(p) malloc_size(p)
#endif

#if defined (__APPLE__) && !defined(MAC_OS_X_VERSION_10_6)
#define ledis_fstat fstat64
#define ledis_stat stat64
#else
#define ledis_fstat fstat
#define ledis_stat stat
#endif

#if defined(__APPLE__) || defined(__linux__)
#define HAVE_BACKTRACE 1
#endif

#endif