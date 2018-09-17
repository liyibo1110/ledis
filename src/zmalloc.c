#include "zmalloc.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "config.h"

static size_t used_memory = 0;

void *zmalloc(size_t size){
    //多申请一段size_t长度的空间
    void *ptr = malloc(size+sizeof(size_t));
    if(!ptr) return NULL;
#ifdef HAVE_MALLOC_SIZE //适配MACOS
    used_memory += ledis_malloc_size(ptr);
    return ptr;
#else
    //在空间size_t的长度里，先写入size的实际大小
    *((size_t*)ptr) = size;
    used_memory += size+sizeof(size_t); //记录增加的大小
    return (char*)ptr+sizeof(size_t);  //返回剩余空间的首地址
#endif
}

void *zrealloc(void *ptr, size_t size){
#ifndef HAVE_MALLOC_SIZE
    void *realptr;
#endif
    size_t oldsize;
    void *newptr;
    if(ptr == NULL) return zmalloc(size);
#ifdef HAVE_MALLOC_SIZE //MACOS
    oldsize = ledis_malloc_size(ptr);   //原来的size值
    newptr = realloc(ptr, size);
    if(!newptr) return NULL;

    used_memory -= oldsize;
    used_memory += ledis_malloc_size(newptr);
    return newptr;
#else
    realptr = (char*)ptr-sizeof(size_t); //这才是头
    oldsize = *((size_t*)realptr);
    newptr = realloc(realptr, size+sizeof(size_t));
    if(!newptr) return NULL;
    
    *((size_t*)newptr) = size;
    used_memory -= oldsize;
    used_memory += size;
    return (char*)newptr+sizeof(size_t);
#endif
}

void zfree(void *ptr){
#ifndef HAVE_MALLOC_SIZE
    void *realptr;
    size_t oldsize;
#endif
    if(ptr == NULL) return;
#ifdef HAVE_MALLOC_SIZE
    used_memory -= ledis_malloc_size(ptr);
    free(ptr);
#else
    realptr = (char*)ptr-sizeof(size_t); //这才是头
    oldsize = *((size_t*)realptr);   //原来的size值
    used_memory -= oldsize+sizeof(size_t);
    free(realptr);
#endif 
}

char *zstrdup(const char *s){
    size_t l = strlen(s)+1; //总长度
    char *p = zmalloc(l);
    memcpy(p, s, l);
    return p;
}

size_t zmalloc_used_memory(void){
    return used_memory;
}