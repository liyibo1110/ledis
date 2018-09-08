#ifndef __ADLIST_H
#define __ADLIST_H

typedef struct listNode{
    struct listNode *prev;
    struct listNode *next;
    void *value;
} listNode;

typedef struct list{
    listNode *head;
    listNode *tail;
    void *(*dup)(void *ptr);    //复制函数实现
    void (*free)(void *ptr);    //释放函数实现
    int (*match)(void *ptr, void *key); //比较函数实现
    unsigned int len;
} list;

typedef struct listIter{
    listNode *next;
    listNode *prev;
    int direction;
} listIter;

/* 定义了相关的操作宏 */
#define listLength(l) ((l)->len)    //返回链表长度
#define listFirst(l) ((l)->head)    //返回链表头元素
#define listLast(l) ((l)->tail)     //返回链表尾元素
#define listPrevNode(n) ((n)->prev) //返回节点的前一个节点
#define listNextNode(n) ((n)->next) //返回节点的下一个节点
#define listNodeValue(n) ((n)->value)   //返回节点的值

#define listSetDupMethod(l,m) ((l)->dup = (m))  //赋予dup函数
#define listSetFreeMethod(l,m) ((l)->free = (m))  //赋予free函数
#define listSetMatchMethod(l,m) ((l)->match = (m))  //赋予match函数

#define listGetDupMethod(l) ((l)->dup)  //赋予dup函数
#define listGetFree(l) ((l)->free)  //赋予free函数
#define listGetMatchMethod(l) ((l)->match)  //赋予match函数

/* 定义原型 */
list *listCreate(void); //初始化
void listRelease(list *list);   //释放
list *listAddNodeHead(list *list, void *value); //头部插入
list *listAddNodeTail(list *list, void *value); //尾部插入
void listDelNode(list *list, listNode *node);   //删除特定节点
listIter *listGetIterator(list *list, int direction);   //获取指定方向的迭代器
listNode *listNextElement(listIter *iter);  //从迭代器返回当前元素
void listReleaseIterator(listIter *iter);   //释放迭代器
list *listDup(list *orig);  //复制链表
listNode *listSearchKey(list *list, void *key); //寻找特定值的节点
listNode *listIndex(list *list, int index); //根据下标返回节点

/* 定义迭代器方向变量 */
#define AL_START_HEAD 0
#define AL_START_TAIL 1

#endif