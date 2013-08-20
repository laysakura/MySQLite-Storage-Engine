#ifndef _PCACHE_H_
#define _PCACHE_H_


#if MYSQLITE_USE_MMAP
#include "pcache_mmap.h"
#else
#include "pcache_malloc.h"
#endif


#endif /* _PCACHE_H_ */
