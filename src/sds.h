#ifndef __SDS_H
#define __SDS_H

#include <sys/types.h>

typedef char *sds;  //只是起个别名，这样普通C字符串也可以当做sds的参数来传递了

struct sdshdr{
    long len;   //字符串实际长度，不包括结尾
    long free;  //字符串容器剩余空间
    char buf[0];    //实际容器，一开始空间为0
};

//开始定义函数接口
sds sdsnewlen(const void *init, size_t initlen);    //初始化
sds sdsnew(const char *init);   //初始化
sds sdsempty(); //清空
size_t sdslen(const sds s); //返回实际长度
sds sdsdup(const sds s);    //复制新的对象
void sdsfree(sds s);    //释放对象
size_t sdsavail(const sds s);   //返回剩余空间
#endif