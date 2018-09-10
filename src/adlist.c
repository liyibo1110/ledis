#include "adlist.h"
#include "zmalloc.h"
#include <stdlib.h>
#include <stdio.h>

list *listCreate(void){
    list *l = zmalloc(sizeof(*l));
    if(l == NULL) return NULL;
    l->head = NULL;
    l->tail = NULL;
    l->dup = NULL;
    l->free = NULL;
    l->match = NULL;
    l->len = 0;
    return l;
}

void listRelease(list *list){
    listNode *next;
    listNode *current = list->head;
    unsigned int len = list->len;
    while(len--){   //遍历释放Node
        next = current->next;
        if(list->free) list->free(current->value);  //定义了free则调用
        zfree(current);
        current = next;
    }
    zfree(list);
}

list *listAddNodeHead(list *list, void *value){
    listNode *node = zmalloc(sizeof(*node));
    if(node == NULL) return NULL;
    node->value = value;
    if(list->len == 0){ //链表如果为空
        node->prev = NULL;
        node->next = NULL;
        list->head = node;
        list->tail = node;
    }else{
        node->prev = NULL;
        node->next = list->head;
        list->head->prev = node;
        list->head = node;
    }
    list->len++;
    return list;
}

list *listAddNodeTail(list *list, void *value){
    listNode *node = zmalloc(sizeof(*node));
    if(node == NULL) return NULL;
    node->value = value;
    if(list->len == 0){ //链表如果为空
        node->prev = NULL;
        node->next = NULL;
        list->head = node;
        list->tail = node;
    }else{
        node->prev = list->tail;
        node->next = NULL;
        list->tail->next = node;
        list->tail = node;
    }
    list->len++;
    return list;
}

void listDelNode(list *list, listNode *node){
    //尝试从链表移除自身
    if(node->prev){
        node->prev->next = node->next;
    }else{  //说明要删的是首元素
        list->head = node->next;
    }
    if(node->next){
        node->next->prev = node->prev;
    }else{  //说明要删的是尾元素
        list->tail = node->prev;
    }
    if(list->free)  list->free(node->value);
    zfree(node);
    list->len--;
}

listIter *listGetIterator(list *list, int direction){
    listIter *iter = zmalloc(sizeof(*iter)); 
    if(iter == NULL) return NULL;
    //指定第一个元素
    if(direction == AL_START_HEAD){
        iter->next = list->head;
    }else{
        iter->next = list->tail;
    }
    iter->direction = direction;
    return iter;    //返回的iter是需要自己在外面释放的
}

listNode *listNextElement(listIter *iter){
    listNode *current = iter->next;
    if(current != NULL){    //有元素则需要移动迭代器，指向下一个元素
        if(iter->direction == AL_START_HEAD){
            iter->next = current->next;
        }else{
            iter->next = current->prev;
        }
    }
    return current;
}

void listReleaseIterator(listIter *iter){
    zfree(iter); //释放自己就够了，不需要释放node
}

list *listDup(list *orig){
    list *copy = listCreate();
    if(copy == NULL) return NULL;
    //先复制函数
    copy->dup = orig->dup;
    copy->free = orig->free;
    copy->match = orig->match;
    listIter *iter = listGetIterator(orig, AL_START_HEAD);

    listNode *node;
    while((node = listNextElement(iter)) != NULL){
        void *value;
        if(copy->dup){  //如果有自定义的dup函数
            value = copy->dup(node->value);
            if(value == NULL){  //只是进行了很粗略的检查
                listRelease(copy);
                listReleaseIterator(iter);
                return NULL;
            }
        }else{
            value = node->value;
        }
        //开始添加节点
        if(listAddNodeTail(copy, value) == NULL){
            listRelease(copy);
            listReleaseIterator(iter);
            return NULL;
        }
    }
    listReleaseIterator(iter);
    return copy;
}

listNode *listSearchKey(list *list, void *key){
    listIter *iter = listGetIterator(list, AL_START_HEAD);
    listNode *node;
    while((node = listNextElement(iter)) != NULL){
        if(list->match){    //如果有自定义的match函数
            if(list->match(node->value, key)){
                listReleaseIterator(iter);
                return node;
            }
        }else{
            if(key == node->value){
                listReleaseIterator(iter);
                return node;
            }
        }
    }
    listReleaseIterator(iter);
    return NULL;
}

listNode *listIndex(list *list, int index){
    listNode *node;
    if(index < 0){  //可以支持为负的下标
        index = (-index)-1;
        node = list->tail;
        while(index-- && node) node = node->prev;
    }else{
        node = list->head;
        while(index-- && node) node = node->next;
    }
    return node;
}