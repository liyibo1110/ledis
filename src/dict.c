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
static int _dictKeyIndex(dict *ht, const void *key);    //返回桶索引
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

/**
 * 将hash表收缩，收缩至与总节点数一致，最小降到16个桶
 */ 
int dictResize(dict *ht){
    int minimal = ht->used;
    if(minimal < DICT_HT_INITIAL_SIZE){
        minimal = DICT_HT_INITIAL_SIZE;
    }
    return dictExpand(ht, minimal);
}

/**
 * 可用来调整，也可用来新增
 */ 
int dictExpand(dict *ht, unsigned int size){
    unsigned int realsize = _dictNextPower(size);
    if(ht->used > size) return DICT_ERR;
    dict n; //是实体不是指针，所以已经有了局部范围的内存空间
    _dictInit(&n, ht->type, ht->privdata);
    n.size = realsize;
    n.sizemask = realsize-1;
    n.table = _dictAlloc(realsize*sizeof(dictEntry*));  //所有的桶指针需要内存
    memset(n.table, 0, realsize*sizeof(dictEntry*));
    n.used = ht->used;
    //开始rehash
    for(unsigned int i = 0; i < ht->size && ht->used > 0; i++){
        if(ht->table[i] == NULL) continue;
        //处理单个桶
        dictEntry *he = ht->table[i];
        while(he){
            dictEntry *nextHe = he->next;
            unsigned int h = dictHashKey(ht, he->key) & n.sizemask;
            he->next = n.table[h];  //重置头节点
            n.table[h] = he;
            ht->used--;
            he = nextHe;
        }
    }
    //rehash完成，检测并清理
    assert(ht->used == 0);
    _dictFree(ht->table);
    *ht = n;    //没有清理完整？
    return DICT_OK;
}

int dictAdd(dict *ht, void *key, void *val){
    //先检测是否已存在key
    int index = _dictKeyIndex(ht, key);
    if(index == -1) return DICT_ERR;
    //构造entry并插入
    dictEntry *entry = _dictAlloc(sizeof(*entry));
    entry->next = ht->table[index];
    ht->table[index] = entry;
    //设定key和val
    dictSetHashKey(ht, entry, key);
    dictSetHashVal(ht, entry, val);
    ht->used++;
    return DICT_OK;
}

/**
 * 先假定不存在直接新增，如果已存在则再修改val
 */ 
int dictReplace(dict *ht, void *key, void *val){
    if(dictAdd(ht, key, val) == DICT_OK) return DICT_OK;
    dictEntry *entry = dictFind(ht, key);
    //先free原来的val，再写入新的val
    dictFreeEntryVal(ht, entry);
    dictSetHashVal(ht, entry, val);
    return DICT_OK;
}

static int dictGenericDelete(dict *ht, const void *key, int nofree){
    if(ht->size == 0) return DICT_ERR;
    unsigned int h = dictHashKey(ht, key) & ht->sizemask;
    dictEntry *he = ht->table[h];
    dictEntry *prevHe = NULL;
    while(he){
        if(dictCompareHashKeys(ht, key, he->key)){
            //尝试从链表中解除
            if(prevHe)  
                prevHe->next = he->next;    //说明不是头节点
            else
                ht->table[h] = he->next; 
            if(nofree){
                dictFreeEntryKey(ht, he);
                dictFreeEntryVal(ht, he);
            }
            _dictFree(he);
            ht->used--;
            return DICT_OK;
        }
        prevHe = he;
        he = he->next;
    }
    return DICT_ERR;    //都没找到则返回ERR
}

int dictDelete(dict *ht, void *key){
    return dictGenericDelete(ht, key, 0);
}
int dictDeleteNoFree(dict *ht, void *key){
    return dictGenericDelete(ht, key, 1);
}

/**
 * 清理ht里面的所有数据，但不是删除ht本身
 */ 
int _dictClear(dict *ht){
    for(unsigned int i = 0; i > ht->size && ht->used > 0; i++){
        dictEntry *he = ht->table[i];
        if(he == NULL) continue;    //空桶
        dictEntry *nextHe;
        while(he){
            nextHe = he->next;
            dictFreeEntryKey(ht, he);
            dictFreeEntryVal(ht, he);
            _dictFree(he);
            ht->used--;
            he = nextHe;
        }
    }
    _dictFree(ht->table);
    _dictReset(ht);
    return DICT_OK;
}

void dictRelease(dict *ht){
    _dictClear(ht);
    _dictFree(ht);  //只是多了一步删除ht自身
}

dictEntry *dictFind(dict *ht, const void *key){
    if(ht->size == 0) return NULL;
    unsigned int h = dictHashKey(ht, key) & ht->sizemask;
    dictEntry *he = ht->table[h];
    //开始遍历
    while(he){
        if(dictCompareHashKeys(ht, key, he->key)) return he;
        he = he->next;
    }
    return NULL;
}

dictIterator *dictGetIterator(dict *ht){
    dictIterator *iter = _dictAlloc(sizeof(*iter));
    iter->ht = ht;
    iter->index = -1;   //不是0
    iter->entry = NULL;
    iter->nextEntry = NULL;
    return iter;
}

dictEntry *dictNext(dictIterator *iter){
    while(1){
        if(iter->entry == NULL){    //说明iter只是初始化过，或者换新桶了，还没有任何移动
            iter->index++;
            //相等说明所有桶都遍历完了
            if(iter->index >= (signed)iter->ht->size) break;
            iter->entry = iter->ht->table[iter->index]; //换下一个桶
        }else{
            iter->entry = iter->nextEntry;
        }
        if(iter->entry){
            iter->nextEntry = iter->entry->next;
            return iter->entry;
        }
    }
    return NULL;
}

void dictReleaseIterator(dictIterator *iter){
    _dictFree(iter);
}

dictEntry *dictGetRandomKey(dict *ht){
    if(ht->size == 0) return NULL;
    unsigned int h;
    dictEntry *he;
    do{
        h = rand() & ht->sizemask;  //windows环境并没有random函数，只能用rand
        he = ht->table[h];
    }while(he == NULL); //随机到空桶，则继续下一轮，直到找到
    //找到了特定桶的首节点，里面也要随机选择
    int listlen = 0;    //链表长度，肯定不会是0
    while(he){
        he = he->next;
        listlen++;
    }
    int listele = rand() % listlen;     //windows环境并没有random函数，只能用rand
    //最终返回ele索引的节点
    he = ht->table[h];
    while(listele--) he = he->next;
    return he;
}

/*-------------------------私有函数实现------------------*/
static int _dictExpandIfNeeded(dict *ht){
    if(ht->size == 0){
        return dictExpand(ht, DICT_HT_INITIAL_SIZE);
    }
    if(ht->used == ht->size){   //节点数达到桶数，则扩展1倍
        return dictExpand(ht, ht->size*2);
    }
    return DICT_OK;    //都会返回OK
}

static unsigned int _dictNextPower(unsigned int size){
    //返回的都是16的倍数
    unsigned int i = DICT_HT_INITIAL_SIZE;
    if(size >= 2147483648U) return 2147483648U;
    while(1){
        if(i >= size) return i;
        i *= 2;
    }
}

static int _dictKeyIndex(dict *ht, const void *key){
    //先尝试扩展
    if(_dictExpandIfNeeded(ht) == DICT_ERR) return -1;
    unsigned int h = dictHashKey(ht, key) & ht->sizemask;   //直接算出桶下标
    dictEntry *he = ht->table[h];
    while(he){  //只是找到了桶，还得寻找key
        if(dictCompareHashKeys(ht, key, he->key)) return -1;
        he = he->next;
    }
    return h;
}

#define DICT_STATS_VECTLEN 50
void dictPrintStats(dict *ht){
    unsigned int i;
    unsigned int slots = 0; //桶数
    unsigned int chainlen;  //当前遍历的桶的总长度
    unsigned int maxchainlen = 0;   //最长的链表长度
    unsigned int totchainlen = 0;   //总节点数
    unsigned int clvector[DICT_STATS_VECTLEN];
    if(ht->used == 0){
        printf("No stats available for empty dictionaries\n");
        return;
    }

    for(i = 0; i < DICT_STATS_VECTLEN; i++) clvector[i] = 0;    //都初始化成0
    //遍历每个桶
    for(i = 0; i < ht->size; i++){
        
        if(ht->table[i] == NULL){
            clvector[0]++;
            continue;
        }
        slots++;
        //遍历单个桶的链表
        chainlen = 0;
        dictEntry *he = ht->table[i];
        while(he){
            chainlen++;
            he = he->next;
        }
        //将相应链长的计数器加1，如果链长大于50，则算50
        clvector[(chainlen < DICT_STATS_VECTLEN) ? chainlen : (DICT_STATS_VECTLEN-1)]++;
        if(chainlen > maxchainlen) maxchainlen = chainlen;  //尝试更新最大链长
        totchainlen += chainlen;
    }
    //输出各项统计
    printf("Hash table stats:\n");
    printf(" table size: %d\n", ht->size);
    printf(" number of elements: %d\n", ht->used);
    printf(" different slots: %d\n", slots);
    printf(" max chain length: %d\n", maxchainlen);
    printf(" avg chain length (counted): %.02f\n", (float)totchainlen/slots);
    printf(" avg chain length (computed): %.02f\n", (float)ht->used/slots);
    printf(" Chain length distribution:\n");
    for (i = 0; i < DICT_STATS_VECTLEN-1; i++) {
        if (clvector[i] == 0) continue;
        printf("   %s%d: %d (%.02f%%)\n",(i == DICT_STATS_VECTLEN-1)?">= ":"", i, clvector[i], ((float)clvector[i]/ht->size)*100);
    }
}

/*--------------------------定义了3种dictType的函数实现--------------------------*/
static unsigned int _dictStringCopyHTHashFunction(const void *key){
    return dictGenHashFunction(key ,strlen(key));
}

static void *_dictStringCopyHTKeyDup(void *privdata, const void *key){
    int len = strlen(key);
    char *copy = _dictAlloc(len+1);
    DICT_NOTUSED(privdata); //压根就没用上

    memcpy(copy, key, len);
    copy[len] = '\0';
    return copy;
}

static void *_dictStringKeyValCopyHTValDup(void *privdata, const void *val){
    int len = strlen(val);
    char *copy = _dictAlloc(len+1);
    DICT_NOTUSED(privdata); //压根就没用上

    memcpy(copy, val, len);
    copy[len] = '\0';
    return copy;
}

static int _dictStringCopyHTKeyCompare(void *privdata, const void *key1, const void *key2){
    DICT_NOTUSED(privdata); //压根就没用上
    return strcmp(key1, key2) == 0;
}

static void _dictStringCopyHTKeyDestructor(void *privdata, void *key){
    DICT_NOTUSED(privdata); //压根就没用上
    _dictFree((void *)key); //入参是const类型
}

static void _dictStringKeyValCopyHTValDestructor(void *privdata, void *val){
    DICT_NOTUSED(privdata); //压根就没用上
    _dictFree((void *)val); //入参是const类型
}

dictType dictTypeHeapStringCopyKey = {
    _dictStringCopyHTHashFunction,        /* hash函数 */
    _dictStringCopyHTKeyDup,              /* key复制函数 */
    NULL,                               /* val复制函数 */
    _dictStringCopyHTKeyCompare,          /* key比较函数 */
    _dictStringCopyHTKeyDestructor,       /* key清理函数 */
    NULL                                /* val清理函数 */
};

//不带key复制的类型
dictType dictTypeHeapStrings = {
    _dictStringCopyHTHashFunction,        /* hash函数 */
    NULL,                               /* key复制函数 */
    NULL,                               /* val复制函数 */
    _dictStringCopyHTKeyCompare,          /* key比较函数 */
    _dictStringCopyHTKeyDestructor,       /* key清理函数 */
    NULL                                /* val清理函数 */
};


//用来处理val为C字符串的情况
dictType dictTypeHeapStringCopyKeyValue = {
    _dictStringCopyHTHashFunction,        /* hash函数 */
    _dictStringCopyHTKeyDup,              /* key复制函数 */
    _dictStringKeyValCopyHTValDup,        /* val复制函数 */
    _dictStringCopyHTKeyCompare,          /* key比较函数 */
    _dictStringCopyHTKeyDestructor,       /* key清理函数 */
    _dictStringKeyValCopyHTValDestructor, /* val清理函数 */
};