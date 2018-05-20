#include "dict.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <assert.h>

/*------------------------工具函数-----------------------*/
/**
 * 普通的打印而已，并不会中断程序
 */ 
static void _dictPanic(const char *fmt, ...){
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "\nDICT LIBRARY PANIC: ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n\n");
    va_end(ap);
}

/*------------------------内存函数-----------------------*/
static void *_dictAlloc(int size){  //为什么不是size_t？
    void *p = malloc(size);
    if(p == NULL){
        _dictPanic("Out of memory");
    }
    return p;
}  

static void _dictFree(void *ptr){
    free(ptr);
}

/*-----------------------私有函数原型--------------------*/
static int _dictExpandIfNeeded(dict *ht);
static unsigned int _dictNextPower(unsigned int size);
static int _dictKeyIndex(dict *ht, const void *key);
static int _dictInit(dict *ht, dictType *type, void *privDataPtr);

/*-------------------------Hash函数---------------------*/

/* Thomas Wang's 32 bit Mix Function */
unsigned int dictIntHashFunction(unsigned int key){
    key += ~(key << 15);
    key ^=  (key >> 10);
    key +=  (key << 3);
    key ^=  (key >> 6);
    key += ~(key << 11);
    key ^=  (key >> 16);
    return key;
}

/* Identity hash function for integer keys */
unsigned int dictIdentityHashFunction(unsigned int key){
    return key;
}

/* Generic hash function (a popular one from Bernstein).
 * I tested a few and this was the best. */
unsigned int dictGenHashFunction(const unsigned char *buf, int len) {
    unsigned int hash = 5381;

    while (len--)
        hash = ((hash << 5) + hash) + (*buf++); /* hash * 33 + c */
    return hash;
}

/*---------------------API实现----------------------*/
static void _dictReset(dict *ht){
    //只重置部分信息而已
    ht->table = NULL;
    ht->size = 0;
    ht->sizemask = 0;
    ht->used = 0;
}

/**
 * 折腾了2圈
 */ 
dict *dictCreate(dictType *type, void *privDataPtr){
    dict *ht = _dictAlloc(sizeof(*ht));
    _dictInit(ht, type, privDataPtr);
    return ht;
}

int _dictInit(dict *ht, dictType *type, void *privDataPtr){
    _dictReset(ht);
    ht->type = type;
    ht->privdata = privDataPtr;
    return DICT_OK;
}

/*-------------------------私有函数实现------------------*/
static unsigned int _dictNextPower(unsigned int size){
    //返回的都是16的倍数
    unsigned int i = DICT_HT_INITIAL_SIZE;
    if(size >= 2147483648U) return 2147483648U;
    while(1){
        if(i >= size) return i;
        i *= 2;
    }
}