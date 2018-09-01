#ifndef __DICT_H
#define __DICT_H

#define DICT_OK 0
#define DICT_ERR 1

#define DICT_NOTUSED(V) ((void) V)  //强制转换避免编译器发出警告

typedef struct dictEntry{
    void *key;
    void *val;
    struct dictEntry *next;
} dictEntry;

typedef struct dictType{
    unsigned int (*hashFunction)(const void *key);
    void *(*keyDup)(void *privdata, const void *key);
    void *(*valDup)(void *privdata, const void *obj);
    int (*keyCompare)(void *privdata, const void *key1, const void *key2);
    void (*keyDestructor)(void *privdata, void *key);
    void (*valDestructor)(void *privdata, void *obj);
} dictType;

typedef struct dict{
    dictEntry **table;
    dictType *type;
    unsigned int size;  //hash桶数
    unsigned int sizemask;  //桶数-1
    unsigned int used;  //共节点数
    void *privdata;
} dict;

typedef struct dictIterator{
    dict *ht;
    int index;  //当前桶索引
    dictEntry *entry;
    dictEntry *nextEntry;
} dictIterator;

#define DICT_HT_INITIAL_SIZE    16

/*-----------------------宏定义---------------------*/
#define dictFreeEntryVal(ht, entry) \
    if((ht)->type->valDestructor)   \
        (ht)->type->valDestructor((ht)->privdata, (entry)->val);

#define dictFreeEntryKey(ht, entry) \
    if((ht)->type->keyDestructor)   \
        (ht)->type->keyDestructor((ht)->privdata, (entry)->key);

#define dictSetHashVal(ht, entry, _val_) do {   \
    if((ht)->type->valDup) \
        (entry)->val = (ht)->type->valDup((ht)->privdata, (_val_)); \
    else    \
        (entry)->val = (_val_);  \
} while(0)

#define dictSetHashKey(ht, entry, _key_) do {   \
    if((ht)->type->keyDup) \
        (entry)->key = (ht)->type->keyDup((ht)->privdata, (_key_)); \
    else    \
        (entry)->key = (_key_);  \
} while(0)

#define dictCompareHashKeys(ht, key1, key2) \
    (((ht)->type->keyCompare) ? \
        (ht)->type->keyCompare((ht)->privdata, key1, key2) : \
        (key1) == (key2))

#define dictHashKey(ht, key) (ht)->type->hashFunction(key)

#define dictGetEntryKey(he) ((he)->key)
#define dictGetEntryVal(he) ((he)->val)
#define dictGetHashTableSize(ht) ((ht)->size)
#define dictGetHashTableUsed(ht) ((ht)->used)

/*--------------------函数定义----------------------*/
dict *dictCreate(dictType *type, void *privDataPtr);    //初始化
int dictExpand(dict *ht, unsigned int size);    //扩展桶数
int dictAdd(dict *ht, void *key, void *val);    //增加键值对
int dictReplace(dict *ht, void *key, void *val);    //修改键值对
int dictDelete(dict *ht, void *key);    //删除键
int dictDeleteNoFree(dict *ht, void *key);  //删除键但不free
void dictRelease(dict *ht); //释放
dictEntry *dictFind(dict *ht, const void *key); //查找entry
int dictResize(dict *ht);
dictIterator *dictGetIterator(dict *ht);    //返回迭代器
dictEntry *dictNext(dictIterator *iter);    //迭代下一个entry
void dictReleaseIterator(dictIterator *iter);
dictEntry *dictGetRandomKey(dict *ht);
void dictPrintStats(dict *ht);
unsigned int dictGenHashFunction(const unsigned char *buf, int len);    //未知
void dictEmpty(dict *ht);

/*---------------------定义3种type--------------------*/
extern dictType dictTypeHeapStringCopyKey;
extern dictType dictTypeHeapStrings;
extern dictType dictTypeHeapStringCopyKeyValue;

#endif