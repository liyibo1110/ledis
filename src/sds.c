#include "sds.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

/**
 * 当内存分配出现问题，打印并退出
 */ 
static void sdsOomAbort(void){
    fprintf(stderr, "SDS: Out Of Memory (SDS_ABORT_ON_OOM defined)\n");
    abort();    //生成core并退出
}


sds sdsnewlen(const void *init, size_t initlen){
    struct sdshdr *sh = malloc(sizeof(struct sdshdr) + initlen + 1);
    #ifdef SDS_ABORT_ON_OOM //开启OOM开关则abort
        if(sh == NULL) sdsOomAbort();
    #else                   //否则只是返回NULL
        if(sh == NULL) return NULL;
    #endif
    sh->len = initlen;
    sh->free = 0;
    if(initlen){    //尝试复制字符串
        if(init){
            memcpy(sh->buf, init, initlen); //buf已经由initlen申请过空间了
        }else{
            memset(sh->buf, 0, initlen);
        }
    }
    //加尾巴
    sh->buf[initlen] = '\0';
    return (char *)sh->buf;
}

sds sdsnew(const char *init){
    size_t initlen = (init == NULL) ? 0 : strlen(init);
    return sdsnewlen(init, initlen);
}

sds sdsempty(){
    return sdsnewlen("", 0);
}

size_t sdslen(const sds s){
    //传入的s是里面的buf，减去原本结构体的大小，可得结构体本身的地址
    struct sdshdr *sh = (void *)(s - sizeof(struct sdshdr));
    return sh->len;
}

sds sdsdup(const sds s){
    return sdsnewlen(s, sdslen(s));
}
void sdsfree(sds s){
    if(s == NULL) return;
    free(s - sizeof(struct sdshdr));
}

size_t sdsavail(const sds s){
    struct sdshdr *sh = (void *)(s - sizeof(struct sdshdr));
    return sh->free;
}

void sdsupdatelen(sds s){
    struct sdshdr *sh = (void *)(s - sizeof(struct sdshdr));
    int reallen = strlen(s);
    sh->free = sh->free + (sh->len - reallen);  //先处理增减free
    sh->len = reallen;
}

/**
 * 尝试将原有的sds增加空间，只是扩展内容，并不负责复制连接内容
 */ 
static sds sdsMakeRoomFor(sds s, size_t addlen){
    size_t free = sdsavail(s);
    size_t len = sdslen(s);
    if(free >= addlen)  return s;   //空间如果还够，直接返回
    struct sdshdr *sh = (void *)(s - sizeof(struct sdshdr));
    //开始扩展
    size_t newlen = (len + addlen) * 2; //扩展后为2倍空间
    struct sdshdr *newsh = realloc(sh, sizeof(struct sdshdr) + newlen + 1);
    #ifdef SDS_ABORT_ON_OOM
        if(newsh == NULL) sdsOomAbort()
    #else
        if(newsh == NULL) return NULL;
    #endif
    //处理剩余
    newsh->free = newlen - len;
    return (char *)newsh->buf;
}

sds sdscatlen(sds s, void *t, size_t len){
    size_t curlen = sdslen(s);  //获取原始长度
    //只是尝试扩展容量
    s = sdsMakeRoomFor(s, len);
    if(s == NULL) return NULL;
    struct sdshdr *sh = (void *)(s - sizeof(struct sdshdr));
    //开始连接字符串
    memcpy(s + curlen, t, len); //注意在原内容后面追加
    sh->len = curlen + len;
    sh->free = sh->free - len;
    s[curlen + len] = '\0';
    return s;
}

sds sdscat(sds s, char *t){
    return sdscatlen(s, t, strlen(t));  //因为参数t是C字符串，所以用strlen就可以了
}

sds sdscpylen(sds s, char *t, size_t len){
    struct sdshdr *sh = (void *)(s - sizeof(struct sdshdr));
    size_t totlen = sh->free + sh->len;
    if(totlen < len){   //总长度都不够存新的，则需要扩展
        s = sdsMakeRoomFor(s, len - totlen);    //注意参数是addlen
        if(s == NULL) return NULL;
        sh = (void *)(s - sizeof(struct sdshdr));
        totlen = sh->free + sh->len;
    }
    memcpy(s, t, len);
    s[len] = '\0';
    sh->len = len;
    sh->free = totlen - len;
    return s;
}

sds sdscpy(sds s, char *t){
    return sdscpylen(s, t, strlen(t));
}

sds sdstrim(sds s, const char *cset){
    struct sdshdr *sh = (void *)(s - sizeof(struct sdshdr));
    char *start = s;    //不变
    char *end = s + sdslen(s) - 1;  //不变
    char *sp = s;   //用来移动
    char *ep = s + sdslen(s) - 1;   //用来移动
    //开始沿2个方向搜索特定字符，找到了则移动指针
    while(sp <= end && strchr(cset, *sp)) sp++;
    while(ep > start && strchr(cset, *ep)) ep--;
    size_t len = (sp > ep) ? 0 : ((ep-sp)+1);   //计算处理后的实际长度
    if(sh->buf != sp){  //如果开头被trim了，则需要整体向前移动，尾部的trim不需要处理
        memmove(sh->buf, sp, len);
    }
    sh->buf[len] = '\0';
    sh->free = sh->free + (sh->len - len);
    sh->len = len;
    return s;
}

sds sdsrange(sds s, long start, long end){
    struct sdshdr *sh = (void *)(s - sizeof(struct sdshdr));
    size_t len = sdslen(s);
    if(len == 0) return s;
    if(start < 0){  //可以处理为负数的start
        start = len + start;
        if(start < 0) start = 0;
    }
    if(end < 0){    //也可以处理为负数的end
        end = len + end;
        if(end < 0) end = 0;
    }
    size_t newlen = (start > end) ? 0 : (end - start) + 1;
    if(newlen != 0){
        if(start >= (signed)len) start = len - 1;
        if(end >= (signed)len) end = len - 1;
        newlen = (start > end) ? 0 : (end - start) + 1;
    }else{
        start = 0;
    }
    if(start != 0) memmove(sh->buf, sh->buf+start, newlen);
    //处理剩余
    sh->buf[newlen] = '\0';
    sh->free = sh->free + (sh->len - newlen);
    sh->len = newlen;
    return s;
}

void sdstolower(sds s){
    int len = sdslen(s);
    for (int j = 0; j < len; j++){
        s[j] = tolower(s[j]);
    }
}

void sdstoupper(sds s){
    int len = sdslen(s);
    for (int j = 0; j < len; j++){
        s[j] = toupper(s[j]);
    }
}

int sdscmp(sds s1, sds s2){
    size_t l1 = sdslen(s1);
    size_t l2 = sdslen(s2);
    size_t minlen = (l1 < l2) ? l1 : l2;
    int cmp = memcmp(s1, s2, minlen);
    if(cmp == 0) return l1-l2;  //如果前面都相等，则按长度比较
    return cmp;
}

/* int main(void){
    sdsnewlen("abc", 3);
} */