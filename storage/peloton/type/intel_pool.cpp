//
// Created by dblab on 2022/2/11.
//
#include "storage/peloton/type/intel_pool.h"
static int Blocks = 0;
static CCC_Header *FreeHeaders = 0;
static int ActiveHeaders = 0;
static int ActiveSizes = 0;
extern CCC_Header CCC_HeaderRear;
CCC_Header CCC_HeaderFront = {
    0,1,1,0,0,&CCC_HeaderRear,0,&CCC_HeaderRear
};
CCC_Header CCC_HeaderRear = {
    static_cast<size_t>(~0),1,1,0,&CCC_HeaderFront,0,&CCC_HeaderFront,0
};

#define HEADERNIL &CCC_HeaderRear

static inline CCC_Header *GetFreeHeader(size_t size)
{
  CCC_Header *fh;
  for (fh=CCC_HeaderFront.rightfree;fh&&fh->size<size;fh=fh->rightfree);
  return fh;
}

inline static void *MoreCore(size_t newblocksize)
{
  void *p = malloc(newblocksize);
  if (p==0) {
    fprintf(stderr,"malloc(%lu) failed.\n",newblocksize);
    exit(1);
  }
  return p;
}

static inline CCC_Header *NearestActiveHeader(size_t size)
{
  CCC_Header *fh;
  for (fh=CCC_HeaderFront.rightactive; fh&&fh->size<size; fh=fh->rightactive);
  return fh;
}

static inline CCC_Header *NewActiveHeader(size_t size)
{
  ActiveSizes++;
  CCC_Header *fh = FreeHeaders;
  if (fh) {
    FreeHeaders = fh->rightactive;
    fh->size = size;
    return fh;
  } else if (ActiveHeaders < MAXACTIVEHEADERS) {
    fh = Headers + ActiveHeaders++;
    fh->size = size;
    return fh;
  } else {
    fprintf(stderr,"Exhausted Freelist headers.\n");
    exit(1);
  }
}

static inline CCC_Header *GetActiveHeader(size_t size)
{
  CCC_Header* r=NearestActiveHeader(size);
  if (r->size == size) {
    return r;
  } else {
    CCC_Header *l=r->leftactive;
    CCC_Header *q=NewActiveHeader(size);
    q->leftactive = l;
    q->rightactive = r;
    l->rightactive = q;
    r->leftactive = q;
    return q;
  }
}

inline void CCC_Header::Push(CCC_FreeObject *p)
{
  if (deallocated==0) {
    CCC_Header *l;
    for (l=this->leftactive;l->deallocated==0;l=l->leftactive);
    CCC_Header *r=l->rightfree;
    this->leftfree = l;
    this->rightfree = r;
    l->rightfree = this;
    r->leftfree = this;
  }
  deallocated += size;
  p->headernum = this - Headers;
  if (!p->rightboundary) {
    CCC_Object *rightneighbor = (CCC_FreeObject*)(((char*)p)+size);
    rightneighbor->leftboundary = 0;
    rightneighbor->predheadernum = this - Headers;
  }
  p->free = 1;
  p->next = freelist;
  p->prev = 0;
  if (freelist) freelist->prev = p;
  freelist = p;
}

inline CCC_Object *CCC_Header::Pop()
{
  CCC_Object *p = freelist;

  freelist = freelist->next;
  if (freelist) freelist->prev = 0;
  deallocated -= size;
  // deallocated为0了，所以从free list中移走
  if (deallocated == 0) {
    this->rightfree->leftfree = this->leftfree;
    this->leftfree->rightfree = this->rightfree;
  }

  return p;
}

static inline CCC_Header *GetActiveHeader(size_t size, CCC_Header *r)
{
  while (r->size > size) r=r->leftactive;
  if (r->size == size) return r;
  CCC_Header *l=r;
  r=l->rightactive;
  CCC_Header *q=NewActiveHeader(size);
  q->leftactive = l;
  q->rightactive = r;
  l->rightactive = q;
  r->leftactive = q;
  return q;
}

inline void CCC_Header::Deliver(CCC_Object *p)
{
  allocated += size;
  p->headernum = this - Headers;
  if (!p->rightboundary) {
    CCC_Object *rightneighbor = (CCC_FreeObject*)(((char*)p)+size);
    rightneighbor->leftboundary = 0;
    rightneighbor->predheadernum = this - Headers;
  }
  p->free = 0;
}

inline void CCC_Header::SpliceOut(CCC_FreeObject *p)
{
  deallocated -= size;
  if (p->prev) {
    p->prev->next = p->next;
  } else {
    freelist = p->next;
  }
  if (p->next) {
    p->next->prev = p->prev;
  }
  if (deallocated == 0) {
    this->rightfree->leftfree = this->leftfree;
    this->leftfree->rightfree = this->rightfree;
  }
}

inline void CCC_Header::Return(CCC_FreeObject *p)
{
  allocated -= size;
  Push(p);
}

inline void CCC_Header::Retract()
{
  allocated -= size;
}

static inline CCC_Header *GetActiveHeader(CCC_Header *l, size_t size)
{
  while (l->size < size) l=l->rightactive;
  if (l->size == size) return l;
  CCC_Header *r=l;
  l=r->leftactive;
  CCC_Header *q=NewActiveHeader(size);
  q->leftactive = l;
  q->rightactive = r;
  l->rightactive = q;
  r->leftactive = q;
  return q;
}

inline void CCC_Header::Dispose()
{
  if (!IsLive()) {
    this->rightactive->leftactive = this->leftactive;
    this->leftactive->rightactive = this->rightactive;
    this->rightactive = FreeHeaders;
    FreeHeaders = this;
    ActiveSizes--;
  }
}

inline int CCC_Header::IsLive()
{
  return allocated || deallocated;
}

inline void *IntelPool::Allocate(size_t request) {
  pool_lock_.Lock();
  request = (request + _CCC_Alignment - 1) & ~(_CCC_Alignment - 1);
  CCC_Object *obj;
  size_t actual;
  actual = request + sizeof(CCC_Object);   // sizeof(_CCC_Object) 16
  if (actual < MINSIZE)   // 32 byte
    actual = MINSIZE;
  else
    actual = (actual + _CCC_Alignment - 1) & ~(_CCC_Alignment - 1);
  TryAgain:
  CCC_Header *p = GetFreeHeader(actual);
  if (p!=HEADERNIL)
  {
    obj = p->Pop();
    size_t bestsize = p->size;
    if ((bestsize - actual) >= MINSIZE)
    {
      size_t returned = bestsize - actual;
      CCC_FreeObject *leftoverobj = (CCC_FreeObject*)(((char*)obj)+actual);
      if (obj->rightboundary)
      {
        obj->rightboundary = 0;
        leftoverobj->rightboundary = 1;
      }
      else
      {
        leftoverobj->rightboundary = 0;
      }
      CCC_Header *q=GetActiveHeader(actual,p);
      q->Deliver(obj);
      CCC_Header *r=GetActiveHeader(returned);
      r->Push(leftoverobj);
      p->Dispose();
    }
    else
    {
      p->Deliver(obj);
    }
    // init_mem(obj->User(), _CCC_AllocatedSize_(obj->User()));
    pool_lock_.Unlock();
    return obj->User();
  }
  else
  {
    size_t nblocks = (actual>>BLOCKSIZELOG) + 1;
    size_t morecoresize = nblocks<<BLOCKSIZELOG;
    CCC_FreeObject* blob = (CCC_FreeObject *)MoreCore(morecoresize);
    Blocks += nblocks;
    blob->rightboundary = 1;
    blob->leftboundary = 1;
    CCC_Header *r=GetActiveHeader(morecoresize);
    r->Push(blob);

    goto TryAgain;
  }

}

inline static void _CCC_Deallocate_To_Header_(void *x)
{
  CCC_FreeObject *p = (CCC_FreeObject *)CCC_Obj(x);
  CCC_FreeObject *l = p->Left();
  CCC_FreeObject *r = p->Right();
  CCC_Header *ph = Headers + p->headernum;
  int coalleft;
  int coalright;
  size_t combsz = ph->size;

  if (r && r->free)
  {
    coalright = 1;
    CCC_Header *rh = Headers + r->headernum;
    rh->SpliceOut(r);
    if (r->rightboundary)
      p->rightboundary = 1;
    combsz += rh->size;
    rh->Dispose();
  }
  else
  {
    coalright = 0;
  }
  if (l && l->free)
  {
    coalleft = 1;
    CCC_Header *lh = Headers + l->headernum;
    lh->SpliceOut(l);
    if (p->rightboundary)
      l->rightboundary = 1;
    p = l;
    combsz += lh->size;
    lh->Dispose();
  }
  else
  {
    coalleft = 0;
  }
  if (coalright || coalleft)
  {
    ph->Retract();
    // we know that combsz > ph->size.  Use GetActiveHeader(_CCC_Object*,size_t);
    CCC_Header *ch = GetActiveHeader(ph,combsz);
    ch->Push(p);
    ph->Dispose();
  }
  else
  {
    ph->Return(p);
  }
}

inline void IntelPool::Free(void *ptr) {
  pool_lock_.Lock();
  _CCC_Deallocate_To_Header_(ptr);
  pool_lock_.Unlock();
}