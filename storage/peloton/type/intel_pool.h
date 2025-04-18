//
// Created by root on 2022/2/21.
//

#pragma once

#include <cstdint>
#include <cstdlib>
#include <unordered_set>

#include "storage/peloton/common/macros.h"
#include "storage/peloton/common/synchronization/spin_latch.h"
#include "storage/peloton/type/abstract_pool.h"

#define _CCC_Alignment sizeof(long long)
#define MAXACTIVEHEADERS 16384
#define CCC_Obj(x) ((CCC_Object *)(((char *)x) - sizeof(CCC_Object)))
#define MINSIZE (sizeof(CCC_FreeObject))
#define BLOCKSIZELOG 20

class CCC_Object {
 public:

  unsigned free : 1;
  unsigned headernum : 14;
  unsigned predheadernum : 14;
  unsigned leftboundary : 1; // if set, then there is no predecessor.
  unsigned rightboundary : 1; // if set, then there is no successor.
  unsigned unused : 1;

  //void *blockmanager;

 private:
  inline CCC_Object *Pred()  { return (CCC_Object*)((char *)this - PredSize()); }
  inline CCC_Object *Succ()  { return (CCC_Object*)((char *)this + Size()); }
  inline CCC_Object *Pred(size_t predsize)  { return (CCC_Object*)((char *)this - predsize); }
  inline CCC_Object *Succ(size_t size)  { return (CCC_Object*)((char *)this + size); }

 public:
  inline size_t Size() ;
  inline size_t PredSize() ;
  inline void *User() ;
  inline CCC_Object *Left()  { if (leftboundary) return 0; else return Pred(); }
  inline CCC_Object *Right()  { if (rightboundary) return 0; else return Succ(); }
  inline CCC_Object *Left(size_t predsize)  { if (leftboundary) return 0; else return Pred(predsize); }
  inline CCC_Object *Right(size_t size)  { if (rightboundary) return 0; else return Succ(size); }

};

class CCC_FreeObject : public CCC_Object {
 public:
  CCC_FreeObject *next;
  CCC_FreeObject *prev;

  inline CCC_FreeObject *Left() { return (CCC_FreeObject*) CCC_Object::Left(); }
  inline CCC_FreeObject *Right() { return (CCC_FreeObject*) CCC_Object::Right(); }
  inline CCC_FreeObject *Left(size_t predsize) { return (CCC_FreeObject*) CCC_Object::Left(predsize); }
  inline CCC_FreeObject *Right(size_t size) { return (CCC_FreeObject*) CCC_Object::Right(size); }
};

struct CCC_Header {
  size_t size;
  size_t allocated;
  size_t deallocated;
  CCC_FreeObject *freelist;
  CCC_Header *leftactive;
  CCC_Header *rightactive;       // also used to string together idle Headers.
  CCC_Header *leftfree;
  CCC_Header *rightfree;

  inline int IsLive();
  inline CCC_Object *Pop();
  inline void Push(CCC_FreeObject *p);
  inline void SpliceOut(CCC_FreeObject *p);
  inline void Deliver(CCC_Object *p);
  inline void Return(CCC_FreeObject *p);
  inline void Retract();
  inline void Dispose();
};


static CCC_Header Headers[MAXACTIVEHEADERS];



inline size_t CCC_Object::Size() 
{
  return Headers[headernum].size;
}

inline size_t CCC_Object::PredSize() 
{
  return Headers[predheadernum].size;
}

inline void* CCC_Object::User()  {
  void* p = (void*) (this + 1);
  return p;
}

//===----------------------------------------------------------------------===//
//
// A memory pool that can quickly allocate chunks of memory to clients.
//
//===----------------------------------------------------------------------===//
class IntelPool : public AbstractPool {
 public:
  IntelPool() = default;

  ~IntelPool();

  static IntelPool &GetInstance();

  void *Allocate(size_t request) override;

  void Free(void *ptr) override;

 public:

  // Spin lock protecting location list
  SpinLatch pool_lock_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

inline IntelPool::~IntelPool() {
  pool_lock_.Lock();

  pool_lock_.Unlock();
}

inline IntelPool &IntelPool::GetInstance() {
  static IntelPool pool;
  return pool;
}