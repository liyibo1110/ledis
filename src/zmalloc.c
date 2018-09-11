#include "zmalloc.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

static size_t used_memory = 0;

void *zmalloc(size_t size){
    //多申请一段size_t长度的空间
    void *ptr = malloc(size+sizeof(size_t));
    //在空间size_t的长度里，先写入size的实际大小
    *((size_t*)ptr) = size;
    used_memory += size+sizeof(size_t); //记录增加的大小
    return (char*)ptr+sizeof(size_t);  //返回剩余空间的首地址
}

void *zrealloc(void *ptr, size_t size){

    if(ptr == NULL) return zmalloc(size);
    void *realptr = (char*)ptr-sizeof(size_t); //这才是头
    size_t oldsize = *((size_t*)realptr);   //原来的size值
    void *newptr = realloc(realptr, size+sizeof(size_t));
    if(!newptr) return NULL;

    *((size_t*)newptr) = size;
    used_memory -= oldsize;
    used_memory += size;
    return (char*)newptr+sizeof(size_t);
}

void zfree(void *ptr){

    if(ptr == NULL) return; 
    void *realptr = (char*)ptr-sizeof(size_t); //这才是头
    size_t oldsize = *((size_t*)realptr);   //原来的size值
    used_memory -= oldsize+sizeof(size_t);
    free(realptr);
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