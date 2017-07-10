#ifdef USE_LMDB
#ifndef CAFFE_UTIL_DB_LMDB_HPP
#define CAFFE_UTIL_DB_LMDB_HPP

#include <string>
#include <vector>
#include <fstream>  // NOLINT(readability/streams)
#include <atomic> // for key_cursor_
#include "lmdb.h"
#include "caffe/util/io.hpp"
#include "caffe/util/rng.hpp"

#include "caffe/util/db.hpp"
#include "caffe/util/format.hpp"
namespace caffe { namespace db {

inline void MDB_CHECK(int mdb_status) {
  CHECK_EQ(mdb_status, MDB_SUCCESS) << mdb_strerror(mdb_status);
} 

class LMDBCursor : public Cursor {
 public:
  explicit LMDBCursor(MDB_txn* mdb_txn, MDB_cursor* mdb_cursor)
    : mdb_txn_(mdb_txn), mdb_cursor_(mdb_cursor), valid_(false) {
    SeekToFirst();
  }
  virtual ~LMDBCursor() {
    mdb_cursor_close(mdb_cursor_);
    mdb_txn_abort(mdb_txn_);
  }
  virtual void SeekToFirst() { Seek(MDB_FIRST); }
  virtual void Next() { Seek(MDB_NEXT); }
  virtual string key() {
    return string(static_cast<const char*>(mdb_key_.mv_data), mdb_key_.mv_size);
  }
  virtual string value() {
    return string(static_cast<const char*>(mdb_value_.mv_data),
        mdb_value_.mv_size);
  }
  virtual bool valid() { return valid_; }

 private:
  void Seek(MDB_cursor_op op) {
    int mdb_status = mdb_cursor_get(mdb_cursor_, &mdb_key_, &mdb_value_, op);
    if (mdb_status == MDB_NOTFOUND) {
      valid_ = false;
    } else {
      MDB_CHECK(mdb_status);
      valid_ = true;
    }
  }

  MDB_txn* mdb_txn_;
  MDB_cursor* mdb_cursor_;
  MDB_val mdb_key_, mdb_value_;
  bool valid_;
};


class RandomAccessLMDBCursor : public Cursor{
 public:
  explicit RandomAccessLMDBCursor(MDB_txn* mdb_txn, MDB_cursor* mdb_cursor, std::string file_name)
    : mdb_txn_(mdb_txn), mdb_cursor_(mdb_cursor), valid_(false) {
    InitKeyList(file_name);
    SeekToFirst();
  }
  virtual ~RandomAccessLMDBCursor() {
    mdb_cursor_close(mdb_cursor_);
    mdb_txn_abort(mdb_txn_);
  }
  // here may seek first key
  virtual void SeekToFirst() { 
    // get first key
    key_cursor_ = 0;
    if (lines_.size() == key_cursor_){
      LOG(ERROR) << "no data in the keys file!";
    }
    setCursorPos();
    
    // call Seek
    Seek(MDB_GET_CURRENT);
    printf("in SeekToFirst\n"); 
    printf("%s\n", mdb_key_.mv_data);
  }
  virtual void Next() { 
    // read next key
    // change cursor positon
    printf("calling next\n");
    key_cursor_++;
    // printf("update key_cursor_ to %lld\n", key_cursor_);
    if (key_cursor_ > lines_.size()){
      setCursorPos();
    }else{
      key_cursor_ = 0;
      // wrap back
      setCursorPos();
    }
    // call Seek
    Seek(MDB_GET_CURRENT); 
  }
  virtual string key() {
    return string(static_cast<const char*>(mdb_key_.mv_data), mdb_key_.mv_size);
  }
  virtual string value() {
    return string(static_cast<const char*>(mdb_value_.mv_data),
        mdb_value_.mv_size);
  }
  virtual bool valid() { return valid_; }
  void InitKeyList(std::string file_name){
    std::ifstream infile(file_name.c_str());
    if(!infile.good()){
      LOG(ERROR) << "Can not find key file when using RandomAccessLMDBCursor";
    }
    // code from caffe convert_imagenet.cpp
    size_t pos;
    int label;
    std::string line;
    while (std::getline(infile, line)) {
      pos = line.find_last_of(' ');
      label = atoi(line.substr(pos + 1).c_str());
      lines_.push_back(std::make_pair(line.substr(0, pos).c_str(), label));
      // printf("%s\n", line.substr(0, pos).c_str());
    }
    //  for (int i = 0; i < lines_.size(); ++i){
    //   printf("%s\n", lines_[i].first.data());
    // }
  }
 private:
  void setCursorPos(){
    // std::string enc = FLAGS_encode_type;
    // if (!enc.size()) {
    //   // Guess the encoding type from the file name
    //   string fn = lines_[key_cursor_].first;
    //   size_t p = fn.rfind('.');
    //   if ( p == fn.npos )
    //     LOG(WARNING) << "Failed to guess the encoding of '" << fn << "'";
    //   enc = fn.substr(p);
    //   std::transform(enc.begin(), enc.end(), enc.begin(), ::tolower);
    // 
    // printf("%lld\n", key_cursor_);
    string key_str = caffe::format_int(key_cursor_, 8) + "_" + lines_[key_cursor_].first;
    // key_str = "00000000_33.jpeg";
   // for (int i = 0; i < lines_.size(); ++i){
   //    printf("%s\n", lines_[i].first.data());
   //  }
    // printf("%s\naaa\n\n", key_str.c_str());
    printf("set cursor positon at %s\n", key_str.c_str());
    MDB_val mdb_key, mdb_data;
    mdb_key.mv_size = key_str.size();
    mdb_key.mv_data = const_cast<char*>(key_str.data());
    mdb_data.mv_size = key_str.size();
    mdb_data.mv_data = const_cast<char*>(key_str.data());
    // SeekTEST(MDB_FIRST);  
    int status = mdb_cursor_get(mdb_cursor_, &mdb_key, &mdb_data, MDB_SET);
    if (0 == status) {
      printf("successfully set the cursor\n");
    }else if (MDB_NOTFOUND == status){
      LOG(ERROR) << "failed to get the cursor, maybe the key is not appeared in the database";
      printf("status: %d\n", status);
      exit(0);
      // printf("%d\n",mdb_cursor_put(mdb_cursor_, &mdb_key, &mdb_data, MDB_FIRST));
    }else if (EINVAL == status){
      LOG(ERROR) << "bad parameter";
      // printf("%d\n",mdb_cursor_put(mdb_cursor_, &mdb_key, &mdb_data, MDB_FIRST));
    }else{
      printf("status: %d\n", status);

    }
  }
  void Seek(MDB_cursor_op op) {
    int mdb_status = mdb_cursor_get(mdb_cursor_, &mdb_key_, &mdb_value_, op);
    if (mdb_status == MDB_NOTFOUND) {
      valid_ = false;
    } else {
      MDB_CHECK(mdb_status);

      valid_ = true;
    }
  }
  void SeekTEST(MDB_cursor_op op) {
    int mdb_status = mdb_cursor_get(mdb_cursor_, &mdb_key_, &mdb_value_, op);
    if (mdb_status == MDB_NOTFOUND) {
      valid_ = false;
    } else {
      MDB_CHECK(mdb_status);

      valid_ = true;
    }
    printf("\n\n\n%s\n%d\n\n\n", mdb_key_.mv_data, mdb_key_.mv_size);
    if (0 == mdb_status) {
      printf("successfully set the cursor at SeekTEST");
    }else if (MDB_NOTFOUND == mdb_status){
      LOG(ERROR) << "failed to get the cursor, maybe the key is not appeared in the database";
      // printf("%d\n",mdb_cursor_put(mdb_cursor_, &mdb_key, &mdb_data, MDB_FIRST));
    }else if (EINVAL == mdb_status){
      LOG(ERROR) << "bad parameter";
      // printf("%d\n",mdb_cursor_put(mdb_cursor_, &mdb_key, &mdb_data, MDB_FIRST));
    }else{
      printf("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    
    exit(0);
    }
  }

  MDB_txn* mdb_txn_;
  MDB_cursor* mdb_cursor_;
  MDB_val mdb_key_, mdb_value_;
  bool valid_;
  std::vector<std::pair<std::string, int> > lines_;
  std::atomic_ullong key_cursor_;
};

class LMDBTransaction : public Transaction {
 public:
  explicit LMDBTransaction(MDB_env* mdb_env)
    : mdb_env_(mdb_env) { }
  virtual void Put(const string& key, const string& value);
  virtual void Commit();

 private:
  MDB_env* mdb_env_;
  vector<string> keys, values;

  void DoubleMapSize();

  DISABLE_COPY_AND_ASSIGN(LMDBTransaction);
};

class LMDB : public DB {
 public:
  LMDB() : mdb_env_(NULL) { }
  virtual ~LMDB() { Close(); }
  virtual void Open(const string& source, Mode mode);
  virtual void Close() {
    if (mdb_env_ != NULL) {
      mdb_dbi_close(mdb_env_, mdb_dbi_);
      mdb_env_close(mdb_env_);
      mdb_env_ = NULL;
    }
  }
  virtual LMDBCursor* NewCursor();
  virtual RandomAccessLMDBCursor* NewRandomAccessCursor(std::string file_name);
  virtual LMDBTransaction* NewTransaction();

 private:
  MDB_env* mdb_env_;
  MDB_dbi mdb_dbi_;
};

}  // namespace db
}  // namespace caffe

#endif  // CAFFE_UTIL_DB_LMDB_HPP
#endif  // USE_LMDB
