// masstree

#include "masstree_index.h"
#include "masstree/masstree_tcursor.hh"

#include "scan_optimizer.h"
#include "storage/peloton/common/container_tuple.h"
//#include "settings/settings_manager.h"
//#include "statistics/backend_stats_context.h"
#include "storage/peloton/store/data_table.h"
#include "storage/peloton/store/storage_manager.h"
#include "storage/peloton/store/tile.h"
//#include "util/portable_endian.h"

MassTreeIndex::MassTreeIndex(IndexMetadata *metadata) : Index(metadata) {
  masstree_threadinfo_main_ = threadinfo::make(threadinfo::TI_MAIN, -1);
  // threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN,-1);
  mcontainer.initialize(*masstree_threadinfo_main_);
//  uint index = std::rand() % 20;
//  masstree_threadinfo_process = threadinfo::make(threadinfo::TI_PROCESS, 0);
  for (uint i = 0; i < MT_PS_COUNT; ++i) {
    threadinfo *masstree_threadinfo_insert = threadinfo::make(threadinfo::TI_PROCESS, i);
    threadinfo *masstree_threadinfo_read = threadinfo::make(threadinfo::TI_PROCESS, i);
    mtps.push_back(masstree_threadinfo_insert);
    mtps_read.push_back(masstree_threadinfo_read);
  }

}

//with tid 传入mysql线程id
bool MassTreeIndex::CondInsertEntry(const Tuple *key, ItemPointer *value,
                                    std::function<bool(const void *)> predicate, uint curr_thd_id) {
//  threadinfo * masstree_threadinfo_process;
//  if(mtps.find(curr_thd_id) != mtps.end()){//存在
//    masstree_threadinfo_process = mtps[curr_thd_id];
//  }else {//不存在
//    masstree_threadinfo_process = threadinfo::make(threadinfo::TI_PROCESS, curr_thd_id);
//    mtps[curr_thd_id] = masstree_threadinfo_process;
//  }
  threadinfo * masstree_threadinfo_process = mtps[curr_thd_id%MT_PS_COUNT];



  std::string temp_s;
  uint align_len = ALIGN8(key->GetKeyLength());
//  uint align_len = ALIGN8(((uint)key->GetColumnCount())*5);

//  uint8_t *buf = (uint8_t*)malloc(ALIGN8(((uint)key->GetColumnCount())*4));
//  uint8_t *buf = (uint8_t*)malloc(ALIGN8(((uint)key->GetColumnCount())*5));
  uint8_t *buf = (uint8_t*)malloc(align_len);
  memset(buf, 0, align_len);

  auto schema = key->GetSchema();

  int offset = 0;
  for(uint i=0;i<key->GetColumnCount();i++){

    if(schema->GetType(i) == TypeId::INTEGER
       ||schema->GetType(i) == TypeId::BIGINT){
      int temp_i ;
      temp_s = key->GetValue(i).ToString();
      //如果是字符串的话, 就是长度4+实际长度. temp_s就是字符串的首地址转为int_8: src
    //然后就提供,dest, src, 实际长度给packkey.
      //offset是int长度4+varchar定义长度.
      temp_i = std::atoi(temp_s.c_str());//
      uintptr_t val = 0;
      Unpack((uint)temp_i, &val);
      PackKey(buf+offset, val);
      offset+=5;
    }else if(schema->GetType(i) == TypeId::VARCHAR){
//      uint var_denfi_len = 10;
      temp_s = key->GetValue(i).ToString();
      size_t len = temp_s.length();

//      uint8_t * temp_uint8 = (uint8_t*)malloc(len);
//      char2uint(temp_s.c_str(),temp_uint8);


//      *(uint32_t*)buf = len;
      memcpy(buf+offset, temp_s.c_str(), len);
//      memcpy(buf+offset, temp_uint8, len);
//      memcpy(buf+4+offset, temp_uint8, len);
//      offset+=(len+4);
      offset+=len;
    }
  }

  // cursor
  Masstree::tcursor<default_table_params> lp(mcontainer, buf, align_len);

//  if (found) result.push_back(reinterpret_cast<ItemPointer *>(lp.value()));
  // Str

//  Masstree::Str Str_key(temp_s);
//  Value v = key->GetValue(0);
//  temp_s = v.ToString();
//  int len = temp_s.length();
//  Masstree::tcursor<default_table_params> lp(mcontainer, Str_key,len);


  // find_insert 这个函数，return
  // true表示要插入的key已经在index里，返回false表示key是新值，并且插入成功
  bool found = lp.find_insert(*masstree_threadinfo_process);
  //__attribute__((unused))

  if(found){
    ItemPointer *old_location = reinterpret_cast<ItemPointer *>(lp.value());
    bool isValid = predicate(old_location);
    if(isValid){
      //这一步是释放find_insert的锁
      lp.finish(1, *masstree_threadinfo_process);
      return false;//当value存在于masstree时, 如果返回的value为valid, 则主键重复.
    }
  }
  //当value存在于masstree时, 如果返回的value为invalid. 则可以插入
  lp.value() =
      reinterpret_cast<Masstree::tcursor<default_table_params>::value_type>(
          value);
  //这一步是释放find_insert的锁
  lp.finish(1, *masstree_threadinfo_process);

  // free(ti);
  return true;
}

//with tid 传入mysql线程id
void MassTreeIndex::Scan(
    UNUSED_ATTRIBUTE const std::vector<Value> &values,
    UNUSED_ATTRIBUTE const std::vector<uint> &key_column_ids,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_types,
    UNUSED_ATTRIBUTE ScanDirectionType scan_direction,
    std::vector<ItemPointer *> &result,
    const ConjunctionScanPredicate *scan_predicate,
    uint curr_thd_id) {

  // Perform the appropriate scan based on the scan predicate
//  if (scan_predicate->IsFullIndexScan()) {
//    ScanAllKeys(result);
//  }
  if (scan_predicate->IsPointQuery()) {
    //    ScanKey(scan_predicate->GetPointQueryKey(), result);
    uint key_length = scan_predicate->GetKeyLength();
    GetKey(values, result, curr_thd_id, key_length);
  }else {
    uint key_length = scan_predicate->GetKeyLength();
    ScanKey(values, result, curr_thd_id, key_length, key_column_ids);
  }
  // no scanRange now
  //    else {
  //      ScanRange(scan_predicate->GetLowKey(), scan_predicate->GetHighKey(),
  //                result);
  //    }

  //   // Update stats
  //   if (static_cast<StatsType>(settings::SettingsManager::GetInt(
  //           settings::SettingId::stats_mode)) != StatsType::INVALID) {
  //     stats::BackendStatsContext::GetInstance()->IncrementIndexReads(
  //         result.size(), GetMetadata());
  //   }
}

//with tid 根据主键查询
void MassTreeIndex::PointGet(const std::vector<Value> &values,
                             std::vector<ItemPointer *> &result, uint key_length,
                             uint curr_thd_id) {
  GetKey(values, result, curr_thd_id, key_length);

}

//with tid 传入mysql线程id
void MassTreeIndex::ScanKey(const std::vector<Value> &values,
                            std::vector<ItemPointer *> &result,
                            uint curr_thd_id, uint key_length,
                            std::vector<uint> key_column_ids) {

  threadinfo * masstree_threadinfo_process = mtps[curr_thd_id%MT_PS_COUNT];
  std::string temp_s;
//  uint align_len =((uint)values.size())*5;
  uint align_len =  key_length;
  align_len = ALIGN8(align_len);
//  align_len = 16;
  uint8_t *buf = (uint8_t*)malloc(align_len);
  uint8_t *buf_next = (uint8_t*)malloc(align_len);
  memset(buf, 0, align_len);
  memset(buf_next, 0, align_len);
  std::vector<const void*> key_void;
  bool hasStrKey = false;

  int offset = 0;

  for (int i = 0; i <values.size() ; ++i) {
    Value v = values.at(i);
    if(v.GetTypeId() == TypeId::INTEGER
        ||v.GetTypeId() == TypeId::BIGINT){
      int temp_i ;
      temp_s = v.ToString();
      //如果是字符串的话, 就是长度4+实际长度. temp_s就是字符串的首地址转为int_8: src
      //然后就提供,dest, src, 实际长度给packkey.
      //offset是int长度4+varchar定义长度.
      temp_i = std::atoi(temp_s.c_str());//
//      int temp_ii = 20;
//      key_void.push_back(&temp_ii);
      key_void.push_back(temp_s.c_str());

      uintptr_t val = 0;
      uintptr_t val_next = 0;
      Unpack((uint)temp_i, &val);
      PackKey(buf+offset, val);
      if(i<values.size()-1){//(1,1,1)和(1,1,2)的区别
        Unpack((uint)temp_i, &val_next);
        PackKey(buf_next+offset, val_next);
      }else {
        Unpack((uint)(temp_i+1), &val_next);
        PackKey(buf_next+offset, val_next);
      }
      offset+=5;
    }else if(v.GetTypeId() == TypeId::VARCHAR){
      hasStrKey = true;
      //      uint var_denfi_len = 10;
      temp_s = v.ToString();

      key_void.push_back(temp_s.c_str());

      size_t len = temp_s.length()-1;//为啥自己创建的value的len要多一个?
//      size_t len = temp_s.length();//为啥自己创建的value的len要多一个?
//      *(uint32_t*)buf = len;
//      memcpy(buf+1+offset, temp_s.c_str(), len);
//      offset+=(len+);
//      memcpy(buf+4+offset, temp_s.c_str(), len);
      memcpy(buf+offset, temp_s.c_str(), len);
      offset+=(len);
//      offset+=(len+4);
    }
  }
  if(values.empty()){
    align_len = 0;
    PackKey(buf, 0);
  }

  TypeId keyTypeId = TypeId::INTEGER;
  if(hasStrKey){
    keyTypeId = TypeId::VARCHAR;
  }
  std::function<bool(ulonglong,uint)> fn = std::bind(&MassTreeIndex::isExist,
                 this, std::placeholders::_1, std::placeholders::_2, key_void, key_column_ids, keyTypeId, align_len);
  std::vector<Masstree::tcursor<default_table_params>::value_type> rs;
  mcontainer.scan(reinterpret_cast<const  char*>(buf),
                  reinterpret_cast<const  char*>(buf_next),
                  align_len,
                  true, fn, *masstree_threadinfo_process, &rs);
  for (uint i = 0; i < rs.size(); ++i) {
    ItemPointer *itemPointer = reinterpret_cast<ItemPointer *>(rs.at(i));
    result.push_back(itemPointer);
  }
  free(buf);
  (void)result;
}

bool MassTreeIndex::isExist(Masstree::tcursor<default_table_params>::value_type value, uint scancount,
                            std::vector<const void*> key, std::vector<uint> key_column_ids, TypeId typeId, uint keylen){

  if(keylen == 0){//keylen = 0 则为索引扫描.
    return true;
  }
//  if(key_column_ids.size() == 1){
//    return true;
//  }

  if(scancount>0){
    return false;
  }

  ItemPointer *current_location = reinterpret_cast<ItemPointer *>(value);

  auto tile_group = StorageManager::GetInstance()->
                    GetTileGroup(current_location->block).get();
  if(tile_group == NULL){
    return false;

  }
  Tile *tile = tile_group->GetTile(0);
  uint tuple_id = current_location->offset;

  const char *tuple_location = tile->GetTupleLocation(tuple_id);
  const Schema *schema = tile->GetSchema();
  uint idx = key_column_ids.at(key_column_ids.size()-1);
  const char *field_location = tuple_location + schema->GetOffset(idx);

  if(typeId == TypeId::VARCHAR){
    const char *ptr = *reinterpret_cast<const char *const *>(field_location);
    const char* from_client = (char*)key.at(key_column_ids.size()-1) ;
    int len = *(int*)ptr;

    char* from_store = (char*)malloc(len+1);//  = ptr+4;
    //const char* from_store = ptr+4;
    memset(from_store, 0, len+1);
    memcpy(from_store,(char*)(ptr+4),len);//去掉空格和空

    if(strcmp(from_store, from_client)!=0){
      return false;
    }
    return true;
  }else {
    int32_t val = *reinterpret_cast<const int32_t *>(field_location);
    int temp_i = std::atoi((char *)key.at(key_column_ids.size()-1));//
    if(temp_i!= val){
      return false;
    }
    return true;
  }

  //  GetAllValueAsBuffer这里参考
  //然后参数里在加一个key
}

void MassTreeIndex::GetKey(const std::vector<Value> &values,
                            std::vector<ItemPointer *> &result,
                            uint curr_thd_id, uint key_length) {

  threadinfo * masstree_threadinfo_process = mtps[curr_thd_id%MT_PS_COUNT];
  std::string temp_s;
  //  uint align_len =((uint)values.size())*5;
  uint align_len =  key_length;
  align_len = ALIGN8(align_len);
  uint8_t *buf = (uint8_t*)malloc(align_len);
  memset(buf, 0, align_len);

  int offset = 0;
  for (Value v: values) {

    if(v.GetTypeId() == TypeId::INTEGER
        ||v.GetTypeId() == TypeId::BIGINT){
      int temp_i ;
      temp_s = v.ToString();
      //如果是字符串的话, 就是长度4+实际长度. temp_s就是字符串的首地址转为int_8: src
      //然后就提供,dest, src, 实际长度给packkey.
      //offset是int长度4+varchar定义长度.
      temp_i = std::atoi(temp_s.c_str());//
      uintptr_t val = 0;
      Unpack((uint)temp_i, &val);
      PackKey(buf+offset, val);
      offset+=5;
    }else if(v.GetTypeId() == TypeId::VARCHAR){
      //      uint var_denfi_len = 10;
      temp_s = v.ToString();
      size_t len = temp_s.length()-1;//为啥自己创建的value的len要多一个?

//      uintptr_t val = 0;
//
//(uint8_t* data, uintptr_t* dest, size_t& len, uint offset)
//      UnpackStr((uint8_t*)temp_s.c_str(), &val, len, offset);
//(uint8_t* dest, uintptr_t src, size_t len, uint offset)
//      PackStr(buf+offset, val, len, offset);

//      uint8_t * temp_uint8 = (uint8_t*)malloc(len);
//      char2uint(temp_s.c_str(),temp_uint8);

//      *(uint32_t*)buf = len;
//      memcpy(buf+4+offset, temp_s.c_str(), len);
//      memcpy(buf+4+offset, temp_uint8, len);
//      offset+=(len+4);

//      memcpy(buf+offset, temp_uint8, len);
      memcpy(buf+offset, temp_s.c_str(), len);
      offset+=(len);

    }
  }
  // cursor
  Masstree::unlocked_tcursor<default_table_params>
      lp(mcontainer, reinterpret_cast<const unsigned char*>(buf), ALIGN8(align_len));
  //      lp(mcontainer, buf, ALIGN8(align_len));
  bool found = lp.find_unlocked(*masstree_threadinfo_process);
    // void* output = lp.value();
    // result.push_back(1);
  if (found) result.push_back(reinterpret_cast<ItemPointer *>(lp.value()));
  free(buf);
  (void)result;
  // free(ti);

}

int MassTreeIndex::char2uint(const char *input, uint8_t *output)
{
  for(int i = 0; i < 24; i++) {
    output[i] &= 0x00;
    for (int j = 1; j >= 0; j--) {
      char hb = input[i*2 + 1 - j];
      if (hb >= '0' && hb <= '9') {
        output[i] |= (uint8_t)((hb - '0') << (4*j));
      } else if (hb >= 'a' && hb <= 'f') {
        output[i] |= (int8_t)((hb - 'a' + 10) << (4*j));
      } else if (hb >= 'A' && hb <= 'F') {
        output[i] |= (int8_t)((hb - 'A' + 10) << (4*j));
      }  else {
        return -1;
      }
    }
  }
  return 0;
}

bool MassTreeIndex::PackKey(uint8_t* dest, uintptr_t src)
{
  uint32_t tmp = (uint32_t)GetBytes4(src);
  *(uint32_t*)(dest + 1) = ntohl(tmp);
  if (tmp & 0x80000000)
    *dest = 0x00;
  else
    *dest = 0x01;

  return true;
}

void MassTreeIndex::Unpack(uint data, uintptr_t* dest)
{
//  *dest = GetBytes4(*(uint32_t*)(data + m_offset));
  *dest = GetBytes4(data);
}

bool MassTreeIndex::PackStr(uint8_t* dest, uintptr_t src, size_t len, uint offset)
{

  *((uint32_t*)(dest + offset)) = len;
  memcpy(dest + offset + 4,  (void*)src, len);
  return true;
}

void MassTreeIndex::UnpackStr(uint8_t* data, uintptr_t* dest, size_t& len, uint offset)
{
//  len = *(uint32_t*)(data + offset);
  *dest = GetBytes8(data + offset + 4);
}


uint MassTreeIndex:: GetAlign8Len(uint a){
  int len =0;
  switch (a) {
    case 1:{
      len = 8;
      break;
    }
    case 2:{
      len = 16;
      break;
    }
    case 3:{
      len = 16;
      break;
    }
    case 4:{
      len = 24;
      break;
    }
    case 5:{
      len = 32;
      break;
    }
  }
  return len;
}

bool MassTreeIndex::InsertEntry(const Tuple *key, ItemPointer *value) {
  // mcontainer.init();

  // Str
  char *temp_s = key->GetData();
  Masstree::Str Str_key(temp_s);
  bool result = false;
  // cursor
  Masstree::tcursor<default_table_params> lp(mcontainer, Str_key);
  // threadinfo *ti = threadinfo::make(threadinfo::TI_PROCESS, 0);
  // find_insert 这个函数，return
  // true表示要插入的key已经在index里，返回false表示key是新值，并且插入成功
  bool found = lp.find_insert(*masstree_threadinfo_process_);
  //__attribute__((unused))

  if (!found) {
    lp.value() = reinterpret_cast<Masstree::tcursor<default_table_params>::value_type>(value);
  }

  lp.finish(1, *masstree_threadinfo_process_);
  result = !found;
  // free(ti);
  return result;
}

bool MassTreeIndex::DeleteEntry(const Tuple *key, ItemPointer *value) {
  (void)key;
  (void)value;
  bool result = false;
  return result;
}

bool MassTreeIndex::CondInsertEntry(const Tuple *key, ItemPointer *value,
                                    std::function<bool(const void *)> predicate) {
  bool result = false;
  // Str
  //  char *temp_s = key->GetData();
  std::string temp_s = key->GetValue(0).ToString();

//  Masstree::Str Str_key(temp_s);

  const char* Str_key = temp_s.data();
  int len = temp_s.length();

  // cursor
  Masstree::tcursor<default_table_params> lp(mcontainer, Str_key, len);
  // threadinfo *ti = threadinfo::make(threadinfo::TI_PROCESS, 0);

  // find_insert 这个函数，return
  // true表示要插入的key已经在index里，返回false表示key是新值，并且插入成功
  bool found = lp.find_insert(*masstree_threadinfo_process_);
  //__attribute__((unused))

  if (!found) {
    lp.value() =
        reinterpret_cast<Masstree::tcursor<default_table_params>::value_type>(
            value);
  }

  lp.finish(1, *masstree_threadinfo_process_);
  result = !found;
  // free(ti);
  (void)predicate;
  (void)result;
  return result;
}

void MassTreeIndex::Scan(
    UNUSED_ATTRIBUTE const std::vector<Value> &values,
    UNUSED_ATTRIBUTE const std::vector<uint> &key_column_ids,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_types,
    UNUSED_ATTRIBUTE ScanDirectionType scan_direction,
    std::vector<ItemPointer *> &result,
    const ConjunctionScanPredicate *scan_predicate) {
  // jy

  // Perform the appropriate scan based on the scan predicate
  if (scan_predicate->IsFullIndexScan()) {
    ScanAllKeys(result);
  } else if (scan_predicate->IsPointQuery()) {
    ScanKey(scan_predicate->GetPointQueryKey(), result);
//    ScanKey(values, result);
  }
  // no scanRange now
  //    else {
  //      ScanRange(scan_predicate->GetLowKey(), scan_predicate->GetHighKey(),
  //                result);
  //    }

  //   // Update stats
  //   if (static_cast<StatsType>(settings::SettingsManager::GetInt(
  //           settings::SettingId::stats_mode)) != StatsType::INVALID) {
  //     stats::BackendStatsContext::GetInstance()->IncrementIndexReads(
  //         result.size(), GetMetadata());
  //   }
}

void MassTreeIndex::ScanLimit(const std::vector<Value> &values,
                              const std::vector<uint> &key_column_ids,
                              const std::vector<ExpressionType> &expr_types,
                              ScanDirectionType scan_direction,
                              std::vector<ItemPointer *> &result,
                              const ConjunctionScanPredicate *scan_predicate,
                              uint64_t limit, uint64_t offset) {
  (void)values;
  (void)key_column_ids;
  (void)expr_types;
  (void)scan_direction;
  (void)result;
  (void)scan_predicate;
  (void)limit;
  (void)offset;
  // This fucking function takes seven fucking arguments ... The fuck?
}

void MassTreeIndex::ScanAllKeys(std::vector<ItemPointer *> &result) {
  (void)result;
}

void MassTreeIndex::ScanKey(const Tuple *key,
                            std::vector<ItemPointer *> &result) {
  // Str
  char *temp_s = key->GetData();
  Masstree::Str Str_key(temp_s);
  // cursor
  Masstree::unlocked_tcursor<default_table_params> lp(mcontainer, Str_key);
  // threadinfo *ti = threadinfo::make(threadinfo::TI_PROCESS, 1);
  bool found = lp.find_unlocked(*masstree_threadinfo_process_);
  // void* output = lp.value();
  // result.push_back(1);
  if (found) result.push_back(reinterpret_cast<ItemPointer *>(lp.value()));
  (void)result;
  // free(ti);

}
//template <typename P>
//int Masstree::basic_table<P>::scan(Str firstkey, bool emit_firstkey,
//                         threadinfo& ti, std::vector<value_type> *results) const
//{
//  return scan(forward_scan_helper(), firstkey, emit_firstkey, ti, results);
//}

