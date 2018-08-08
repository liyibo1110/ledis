#define _GNU_SOURCE
#include "anet.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <ctype.h>
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

/**
 * 连接给定的addr和port端口，用的是新版的API重写，而不是redis自己默认的老版本
 */ 
int anetTcpConnect(char *err, char *addr, int port){

    //初始化
    struct addrinfo hints;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_canonname = NULL;
    hints.ai_addr = NULL;
    hints.ai_next = NULL;
    hints.ai_family = AF_INET;  //只支持IPv4
    hints.ai_socktype = SOCK_DGRAM; //只支持TCP流
    hints.ai_flags = AI_NUMERICSERV;    //service只能是数字，不需要走转换
    struct addrinfo *result, *rp;

    //要将port转换成字符串形式，最多5+1位就够了
    char portBuf[6];
    snprintf(portBuf, sizeof(portBuf), htons(port));
    int s =  getaddrinfo(addr, portBuf, &hints, &result);
    if(s != 0){
        anetSetError(err, "getaddrinfo: %s\n", strerror(errno));
        return ANET_ERR;
    }

    int sfd;
    //开始循环遍历结果，最终确定socket的描述符
    for(rp = result; rp != NULL; rp = rp->ai_next){
        sfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if(sfd == -1)   continue;
        if(connect(sfd, rp->ai_addr, rp->ai_addrlen) != -1) break;  //找到了就不用再找了
        //不成功还得关闭
        close(sfd);
    }
    freeaddrinfo(result);

    if(rp == NULL){
        anetSetError(err, "connect: %s\n", strerror(errno));
        return ANET_ERR;
    }

    return sfd;
}

/**
 * 以给定的port端口开启服务端，用的是新版的API重写，而不是redis自己默认的老版本
 */ 
int anetTcpServer(char *err, int port, char *bindaddr){
    
    //初始化
    struct addrinfo hints;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_canonname = NULL;
    hints.ai_addr = NULL;
    hints.ai_next = NULL;
    hints.ai_family = AF_INET;  //只支持IPv4
    hints.ai_socktype = SOCK_DGRAM; //只支持TCP流
    if(!bindaddr)   hints.ai_flags = AI_PASSIVE;    //没传bindaddr，则自动使用通配符IP
    struct addrinfo *result, *rp;

    //要将port转换成字符串形式，最多5+1位就够了
    char portBuf[6];
    snprintf(portBuf, sizeof(portBuf), htons(port));
    int s;
    if(bindaddr){
        s = getaddrinfo(bindaddr, portBuf, &hints, &result);
    }else{
        s = getaddrinfo(NULL, portBuf, &hints, &result);
    }
    
    if(s != 0){
        anetSetError(err, "getaddrinfo: %s\n", strerror(errno));
        return ANET_ERR;
    }

    int on = 1;
    int sfd;
    //开始循环遍历结果，最终确定socket的描述符
    for(rp = result; rp != NULL; rp = rp->ai_next){
        sfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if(sfd == -1)   continue;
        //开通REUSEADDR标记，使得可以复用TIME_WAIT类型的TCP资源
        if(setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) == -1){
            anetSetError(err, "setsockopt SO_REUSEADDR: %s\n", strerror(errno));
            close(sfd);
            freeaddrinfo(result);
            return ANET_ERR;
        }
        //开始bind到本机地址
        if(bind(sfd, rp->ai_addr, rp->ai_addrlen) == 0) break;
        close(sfd); 
    }
    
    if(rp == NULL){
        anetSetError(err, "bind: %s\n", strerror(errno));
        return ANET_ERR;
    }

    //最后开始监听
    if(listen(sfd, 5) == -1){
        anetSetError(err, "listen: %s\n", strerror(errno));
        close(sfd);
        return ANET_ERR;
    }

    return sfd;
}

/**
 * 以给定的sfd开始阻塞请求，用的是新版的API重写，而不是redis自己默认的老版本
 * 当有请求到来，函数会返回cfd，同时会填充ip和port缓冲区（需要调用者自行处理长度问题）
 */ 
int anetAccept(char *err, int serversock, char *ip, int *port){
    int cfd;
    struct sockaddr_storage claddr;
    while(true){
        cfd = accept(serversock, (struct sockaddr *)&claddr, sizeof(struct sockaddr_storage));
        if(cfd == -1){
            if(errno == EINTR){
                continue;
            }else{
                anetSetError(err, "accept: %s\n", strerror(errno));
                return ANET_ERR;
            }
        }
        break;
    }
    //不管要不要，都尝试获取客户端地址，然后按需填充
    char host[NI_MAXHOST], service[NI_MAXSERV];
    if(getnameinfo(&claddr, sizeof(struct sockaddr_storage), host, NI_MAXHOST, 
                    service, NI_MAXSERV, (NI_NUMERICHOST | NI_NUMERICSERV)) == 0){
        if(ip){
            snprintf(ip, NI_MAXHOST, host);
        }
        if(port){
            *port = atoi(service);
        }
    }else{  //获取失败
        if(ip){
            snprintf(ip, NI_MAXHOST, "(?UNKNOWN?)");
        }
        if(port){
            *port = 0;
        }
    }

    return cfd;
}

