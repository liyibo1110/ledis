#ifndef __SDS_H
#define __SDS_H

#include <sys/types.h>

typedef char *sds;  //只是起个别名，这样普通C字符串也可以当做sds的参数来传递了

struct sdshdr{
    long len;   //字符串实际长度，不包括结尾
    long free;  //字符串容器剩余空间
    char buf[0];    //实际容器，一开始空间为0，因此sizeof(struct sdshdr)的结果是8
};

//开始定义函数接口
sds sdsnewlen(const void *init, size_t initlen);    //初始化
sds sdsnew(const char *init);   //C字符串初始化
sds sdsempty(); //初始化空对象

size_t sdslen(const sds s); //返回实际长度
sds sdsdup(const sds s);    //复制新的对象
void sdsfree(sds s);    //释放对象
size_t sdsavail(const sds s);   //返回剩余空间

sds sdscatlen(sds s, void *t, size_t len);  //带长度的拼接
sds sdscat(sds s, char *t); //C字符串直接拼接

sds sdscpylen(sds s, char *t, size_t len);  //带长度的覆盖
sds sdscpy(sds s, char *t); //C字符串直接覆盖

//sds sdscatprintf(sds s, const char *fmt, ...);  //带模版的拼接
sds sdstrim(sds s, const char *cset);   //清理首尾2端特定的字符
sds sdsrange(sds s, long start, long end);  //截取区间字符串

void sdsupdatelen(sds s);   //重新计算sds里面的len和free值
int sdscmp(sds s1, sds s2); //比较2个字符串

sds *sdssplitlen(char *s, int len, char *sep, int seplen, int *count);  //将C字符串分隔成sds数组
void sdstolower(sds s); //转小写，直接操作原始sds
void sdstoupper(sds s); //转大写，直接操作原始sds
#endif