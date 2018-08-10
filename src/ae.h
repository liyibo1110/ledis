#ifndef __AE_H
#define __AE_H

#define AE_OK 0
#define AE_ERR -1

#define AE_READABLE 1
#define AE_WRITABLE 2
#define AE_EXCEPTION 4

#define AE_FILE_EVENTS 1
#define AE_TIME_EVENTS 2
#define AE_ALL_EVENTS (AE_FILE_EVENTS | AE_TIME_EVENTS)
#define AE_DONT_WAIT 4

#define AE_NOMORE -1

//消除编译器转换警告
#define AE_NOTUSED(V) ((void) V)

//先有鸡还是先有蛋的问题，不声明就啥都没有
struct aeEventLoop;

typedef void aeFileProc(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
typedef int aeTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData);
typedef void aeEventFinalizerProc(struct aeEventLoop *eventLoop, void *clientData);

//文件相关事件类型
typedef struct aeFileEvent{
    int fd;
    int mask;   //AE_READABLE或者AE_WRITABLE或者AE_EXCEPTION，select调用需要
    aeFileProc *fileProc;   //处理函数指针
    aeEventFinalizerProc *finalizeProc; //清理函数指针
    void *clientData;
    struct aeFileEvent *next;   //下一个event
} aeFileEvent;

//时间相关事件类型
typedef struct aeTimeEvent{
    long long id;   //标识符
    long when_sec;
    long when_ms;
    aeTimeProc *timeProc;   //处理函数指针
    aeEventFinalizerProc *finalizeProc; //清理函数指针
    void *clientData;
    struct aeTimeEvent *next;   //下一个event
} aeTimeEvent;

//事件容器集合
typedef struct aeEventLoop{
    long long timeEventNextId;  //下一个event编号
    aeFileEvent *fileEventHead; //file事件头元素
    aeTimeEvent *timeEventHead; //time时间头元素
    int stop;   //是否停止
} aeEventLoop;

aeEventLoop *aeCreateEventLoop(void);
void aeDeleteEventLoop(aeEventLoop *eventLoop);
void aeStop(aeEventLoop *eventLoop);

int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
                        aeFileProc *proc, void *clientData, 
                        aeEventFinalizerProc *finalizerProc);
int aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask);

int aeCreateTimeEvent(aeEventLoop *eventLoop, long milliseconds,
                        aeTimeProc *proc, void *clientData, 
                        aeEventFinalizerProc *finalizerProc);
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id);

int aeProcessEvents(aeEventLoop *evnetLoop, int flags);
void aeMain(aeEventLoop *eventLoop);

#endif