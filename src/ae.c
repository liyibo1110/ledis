#include "ae.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <string.h>
#include <stdbool.h>
#include <sys/time.h>
#include <time.h>
#include <sys/select.h>
#include <errno.h>

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
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask){
    
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
static void aeAddMillisecondsToNow(long long milliseconds, long *sec, long *ms){
    long cur_sec, cur_ms;
    //long when_sec, when_ms;
    //获取当前时间
    aeGetTime(&cur_sec, &cur_ms);
    long when_sec = cur_sec + milliseconds/1000;
    long when_ms = cur_ms + milliseconds%1000; 
    //增加完毕，需要将毫秒进位到秒
    if(when_ms >= 1000){
        when_sec++;
        when_ms -= 1000;
    }
    *sec = when_sec;
    *ms = when_ms;
}

/**
 * 新增一个TimeEvent，只是简单赋值，并且加入到EventLoop的首元素
 */ 
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long milliseconds,
                        aeTimeProc *proc, void *clientData, 
                        aeEventFinalizerProc *finalizerProc){
    
    long long id = eventLoop->timeEventNextId++;    //第一个id是从0开始
    aeTimeEvent *te = malloc(sizeof(*te));
    if(te == NULL)  return AE_ERR;
    te->id = id;
    aeAddMillisecondsToNow(milliseconds, &te->when_sec, &te->when_ms);
    te->timeProc = proc;
    te->finalizeProc = finalizerProc;
    te->clientData = clientData;
    te->next = eventLoop->timeEventHead;
    eventLoop->timeEventHead = te;
    return id;
}

/**
 * 删除特定的TimeEvent，删一个就会退出
 */ 
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id){
    
    aeTimeEvent *prev = NULL;
    aeTimeEvent *te = eventLoop->timeEventHead;
    //从头挨个找
    while(te){
        if(te->id == id){
            if(prev == NULL){   //说明是首元素
                eventLoop->timeEventHead = te->next;
            }else{  //说明不是首元素，prev一定有值
                prev->next = te->next;
            }
            if(te->finalizeProc){
                te->finalizeProc(eventLoop, te->clientData);
            }
            free(te);
            return AE_OK;
        }
        //不是就尝试下一个，需要记住上一个结构
        prev = te;
        te = te->next;
    }
    return AE_ERR;  //不应该找不到相应的id
}

/**
 * 返回一个事件容器里，最近要到期的1个TimeEvent
 */ 
static aeTimeEvent *aeSearchNearestTimer(aeEventLoop *eventLoop){
    aeTimeEvent *te = eventLoop->timeEventHead;
    aeTimeEvent *nearest = NULL;
    while(te){
        //满足以下任意条件，当前event会成为最近的event
        if(!nearest || te->when_sec < nearest->when_sec 
                    || (te->when_sec == nearest->when_sec && te->when_ms < nearest->when_ms)){
            nearest = te;
        }
        te = te->next;
    }
    return nearest;
}

/**
 * 遍历给定的事件容器
 */ 
int aeProcessEvents(aeEventLoop *eventLoop, int flags){

    //2个event如果都不传，直接退出
    if(!(flags & AE_TIME_EVENTS) && !(flags & AE_FILE_EVENTS))    return 0;

    fd_set rfds, wfds, efds;
    //必须先初始化
    FD_ZERO(&rfds);
    FD_ZERO(&wfds);
    FD_ZERO(&efds);

    //如果特意传了file标记，则要先处理（加入到select监控列表）
    int maxfd = 0, numfd = 0, processed = 0;
    aeFileEvent *fe = eventLoop->fileEventHead;
    if(flags & AE_FILE_EVENTS){
        //尝试遍历并根据mask，加入相应的监控列表
        while(fe != NULL){
            if(fe->mask & AE_READABLE)  {
                printf("fd: %d has put rfds\n", fe->fd);
                FD_SET(fe->fd, &rfds);
            }
            if(fe->mask & AE_WRITABLE)  {
                printf("fd: %d has put wfds\n", fe->fd);
                FD_SET(fe->fd, &wfds);
            }
            if(fe->mask & AE_EXCEPTION) {
                printf("fd: %d has put efds\n", fe->fd);
                FD_SET(fe->fd, &efds);
            }
            //寻找最大的fd，为了满足select
            if(maxfd < fe->fd)  maxfd = fe->fd;
            numfd++;
            //printf("maxfd=%d\n", maxfd);
            fe = fe->next;
        }
    }

    
    if(numfd || ((flags & AE_TIME_EVENTS) && !(flags & AE_DONT_WAIT))){

        //printf("numfd=%d\n", numfd);

        //尝试找出最近要到达的1个timeEvent
        aeTimeEvent *shortest = NULL;
        struct timeval tv, *tvp;
        if(flags & AE_TIME_EVENTS && !(flags & AE_DONT_WAIT)){
            shortest = aeSearchNearestTimer(eventLoop); //有可能在这里就已经超时，select会返回-1
        }
        if(shortest){   //计算出与当前的时间差并保存
            long now_sec, now_ms;
            aeGetTime(&now_sec, &now_ms);
            tvp = &tv;
            //printf("tvp->tv_sec=%ld\n", (long)shortest->when_sec - now_sec);
            tvp->tv_sec = shortest->when_sec - now_sec;
            //处理毫秒差，要考虑秒退位的问题，还要注意timeval结构里面存的是微秒
            if(shortest->when_ms < now_ms){
                tvp->tv_usec = ((shortest->when_ms+1000) - now_ms)*1000;
                tvp->tv_sec--;
            }else{
                tvp->tv_usec = (shortest->when_ms - now_ms)*1000;
            }
        }else{  //如果没有timeEvent，则根据DONT_WAIT标记来决定时间
            if(flags & AE_DONT_WAIT){   //传了DONT_WAIT，则设定为select立即返回
                tv.tv_sec = 0;
                tv.tv_usec = 0;
                tvp = &tv;
            }else{  //没有DONT_WAIT标记，同时也没有timeEvent存在，则select会一直阻塞
                tvp = NULL;
            }
        }

        //tvp时间有可能是负数（例如第一次启动在loadDb操作花费了超过1s的时间），这样retval会为-1,程序并没有处理错误，而是继续
        int retval = select(maxfd+1, &rfds, &wfds, &efds, tvp);
        printf("select is over, retval=%d\n", retval);
        if(retval > 0){

            //还得遍历event集合，找出哪些就绪了
            fe = eventLoop->fileEventHead;
            while(fe != NULL){
                int fd = fe->fd;
                if((fe->mask & AE_READABLE && FD_ISSET(fd, &rfds)) ||
                    (fe->mask & AE_WRITABLE && FD_ISSET(fd, &wfds)) ||
                    (fe->mask & AE_EXCEPTION && FD_ISSET(fd, &efds))){
                    
                    int mask = 0;
                    if(fe->mask & AE_READABLE && FD_ISSET(fd, &rfds))   mask |= AE_READABLE;
                    if(fe->mask & AE_WRITABLE && FD_ISSET(fd, &wfds))   mask |= AE_WRITABLE;
                    if(fe->mask & AE_EXCEPTION && FD_ISSET(fd, &efds))   mask |= AE_EXCEPTION;
                    //执行相应的处理函数
                    //printf("run fileProc\n");
                    fe->fileProc(eventLoop, fe->fd, fe->clientData, mask);
                    //printf("run fileProc over\n");
                    processed++;
                    //如果处理过里面的event，整个eventLoop可能会有变化，所以需要重头再次遍历
                    fe = eventLoop->fileEventHead;
                    FD_CLR(fd, &rfds);
                    FD_CLR(fd, &wfds);
                    FD_CLR(fd, &efds);
                }else{
                    fe = fe->next;
                }
            }
        }
    }

    //最后还要处理timeEvent
    if(flags & AE_TIME_EVENTS){
        aeTimeEvent *te = eventLoop->timeEventHead;
        long long maxId = eventLoop->timeEventNextId-1; //id是从0开始的
        while(te){
            if(te->id > maxId){
                te = te->next;  //保证maxId每次不变，即每次最多只处理maxId个event
                continue;
            }
            long now_sec, now_ms;
            long long id;
            aeGetTime(&now_sec, &now_ms);

            if(now_sec > te->when_sec ||
                (now_sec == te->when_sec && now_ms >= te->when_ms)){    //如果timeEvent到期了

                id = te->id;
                //执行timeEvent相应的处理函数，这里是返回值，是下次到期要增加的毫秒数，如果返回AE_NOMORE则说明event是一次性的，直接清理即可
                int retval = te->timeProc(eventLoop, id, te->clientData);
                if(retval != AE_NOMORE){
                    aeAddMillisecondsToNow(retval, &te->when_sec, &te->when_ms);
                }else{
                    aeDeleteTimeEvent(eventLoop, id);
                }
                //和fileEvent一样，处理了可能会改变eventLoop，所以要重头再来
                te = eventLoop->timeEventHead;
            }else{
                te = te->next;
            }
        }
    }
    printf("processed=%d\n", processed);
    return processed;   //本次一共处理了多少个fileEvent
}

/**
 * 立即尝试调用一次select轮询，查看给定的单个fd是否就绪，timeout参数由传入的时间参数指定
 */ 
int aeWait(int fd, int mask, long long milliseconds){
    
    struct timeval tv;
    tv.tv_sec = milliseconds/1000;
    tv.tv_usec = (milliseconds%1000)*1000;

    fd_set rfds, wfds, efds;
    FD_ZERO(&rfds);
    FD_ZERO(&wfds);
    FD_ZERO(&efds);
    if(mask & AE_READABLE) FD_SET(fd, &rfds);
    if(mask & AE_WRITABLE) FD_SET(fd, &wfds);
    if(mask & AE_EXCEPTION) FD_SET(fd, &efds);
    int retmask = 0, retval;
    if((retval = select(fd+1, &rfds, &wfds, &efds, &tv)) > 0){
        //这个fd有就绪的
        if(FD_ISSET(fd, &rfds)) retmask |= AE_READABLE;
        if(FD_ISSET(fd, &wfds)) retmask |= AE_WRITABLE;
        if(FD_ISSET(fd, &efds)) retmask |= AE_EXCEPTION;
        return retmask;
    }else{
        return retval;  //没就绪就是0,负数说明出错了
    }
}

/**
 * 启动一个event容器，循环执行直到stop置位
 */ 
void aeMain(aeEventLoop *eventLoop){
    eventLoop->stop = 0;
    while(!eventLoop->stop){
        aeProcessEvents(eventLoop, AE_ALL_EVENTS);
    }
}

/* int main(int argc, char *argv[]){
    int i = 0;
    int j = i++;
    printf("j=%d\n", j);
    exit(EXIT_SUCCESS);
} */


