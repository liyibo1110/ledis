#include "anet.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <stdarg.h>
#include <stdbool.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>

/**
 * 将错误信息放到传来的err指针里，并不会实际返回任何值
 */ 
static void anetSetError(char *err, const char *fmt, ...){
    if(!err)    return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(err, ANET_ERR_LEN, fmt, ap);
    va_end(ap);
}

/**
 * 和read调用参数一样，区别是它会循环尝试读满count个字节，除非出错了或者EOF了
 */ 
int anetRead(int fd, void *buf, int count){
    int numRead;
    int totalRead = 0;
    while(totalRead != count){
        numRead = read(fd, buf, count - totalRead);
        if(numRead == 0)    return totalRead;//EOF
        if(numRead == -1)   return -1;
        totalRead += numRead;
        buf += numRead; //缓冲区必须也得位移
    }
    return totalRead;
}

/**
 * 和write调用参数一样，区别是它会循环尝试写满count个字节，除非出错了或者EOF了
 */ 
int anetWrite(int fd, void *buf, int count){
    int numWritten;
    int totalWritten = 0;
    while(totalWritten != count){
        numWritten = write(fd, buf, count - numWritten);
        if(numWritten == 0) return totalWritten;
        if(numWritten == -1)    return -1;
        totalWritten += totalWritten;
        buf += numWritten;
    }
    return totalWritten;
}