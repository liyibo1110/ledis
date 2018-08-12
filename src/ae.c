#include "ae.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdbool.h>
#include <sys/time.h>

/**
 * 新建并初始化一个EventLoop，都没有值
 */ 
aeEventLoop *aeCreateEventLoop(void){
    aeEventLoop *eventLoop = malloc(sizeof(*eventLoop));
    if(eventLoop == NULL)   return NULL;
    eventLoop->fileEventHead = NULL;
    eventLoop->timeEventHead = NULL;
    eventLoop->timeEventNextId = 0;
    eventLoop->stop = 0;
    return eventLoop;
}

/**
 * 释放EventLoop
 */ 
void aeDeleteEventLoop(aeEventLoop *eventLoop){
    free(eventLoop);
}

/**
 * 停止EventLoop，只是将stop字段置为1
 */ 
void aeStop(aeEventLoop *eventLoop){
    eventLoop->stop = 1;
}

/**
 * 新增一个FileEvent，只是简单赋值，并且加入到EventLoop的首元素
 */ 
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
                        aeFileProc *proc, void *clientData, 
                        aeEventFinalizerProc *finalizerProc){
    aeFileEvent *fe = malloc(sizeof(*fe));
    if(fe == NULL)  return AE_ERR;
    fe->fd = fd;
    fe->mask = mask;
    fe->fileProc = proc;
    fe->finalizeProc = finalizerProc;
    fe->clientData = clientData;
    fe->next = eventLoop->fileEventHead;
    eventLoop->fileEventHead = fe;
    return AE_OK;
}

/**
 * 删除特定的FileEvent，删一个就会退出
 */ 
void aeDeleteEventLoop(aeEventLoop *eventLoop, int fd, int mask){
    
    aeFileEvent *prev = NULL;
    aeFileEvent *fe = eventLoop->fileEventHead;
    //从头挨个找
    while(fe){
        if(fe->fd == fd && fe->mask == mask){
            if(prev == NULL){   //说明是首元素
                eventLoop->fileEventHead = fe->next;
            }else{  //说明不是首元素，prev一定有值
                prev->next = fe->next;
            }
            if(fe->finalizeProc){
                fe->finalizeProc(eventLoop, fe->clientData);
            }
            free(fe);
            return;
        }
        //不是就尝试下一个，需要记住上一个结构
        prev = fe;
        fe = fe->next;
    }
}

/**
 * 取当前时间，填充传入的秒和毫秒缓冲区
 */ 
static void aeGetTime(long *seconds, long *milliseconds){
    struct timeval tv;
    gettimeofday(&tv, NULL);
    *seconds = tv.tv_sec;
    *milliseconds = tv.tv_usec / 1000;  //usec是微秒，需要转成毫秒
}

/**
 * 将传入的时间缓冲区，增加毫秒数
 */ 
static void adAddMillisecondsToNow(long long milliseconds, long *sec, long *ms){
    long currentSeconds, currentMilliSeconds;
    //long whenSeconds, whenMilliSeconds;
    //获取当前时间
    aeGetTime(&currentSeconds, &currentMilliSeconds);
    long whenSeconds = currentSeconds + milliseconds / 1000;
    long whenMilliSeconds = currentMilliSeconds + milliseconds % 1000; 
    //增加完毕，需要将毫秒进位到秒
    if(whenMilliSeconds >= 1000){
        whenSeconds++;
        whenMilliSeconds -= 1000;
    }
    *sec = whenSeconds;
    *ms = whenMilliSeconds;
}


