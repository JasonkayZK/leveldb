#include <cassert>
#include <iostream>

#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"

#include "gtest/gtest.h"

const std::string db_name = "/tmp/test_db";

leveldb::Options get_options() {
  leveldb::Options options;
  options.create_if_missing = true;
  return options;
}

leveldb::DB* init_db(leveldb::Options&& options) {
  leveldb::DB* db;
  leveldb::Status status = leveldb::DB::Open(options, db_name, &db);
  assert(status.ok());
  return db;
}

TEST(LevelDBDemo, OpenDB) {
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  //  options.error_if_exists = true;
  leveldb::Status status = leveldb::DB::Open(options, db_name, &db);

  std::cout << "db started, status: " << status.ToString() << std::endl;
  ASSERT_TRUE(status.ok());
  delete db;
}

TEST(LevelDBDemo, CRUD) {
  leveldb::DB* db = init_db(get_options());
  std::string k = "name";

  // Write data.
  leveldb::Status status = db->Put(leveldb::WriteOptions(), k, "test");
  ASSERT_TRUE(status.ok());

  // Read data.
  std::string val;
  status = db->Get(leveldb::ReadOptions(), k, &val);
  ASSERT_TRUE(status.ok());
  std::cout << "Get key: " << k << ", val: " << val << std::endl;
  ASSERT_EQ(val, "test");

  // Delete data.
  status = db->Delete(leveldb::WriteOptions(), "name");
  ASSERT_TRUE(status.ok());

  // Re-Get the key:
  status = db->Get(leveldb::ReadOptions(), k, &val);
  ASSERT_FALSE(status.ok());
  std::cout << "Get key after delete, status: " << status.ToString()
            << std::endl;
  ASSERT_TRUE(status.IsNotFound());

  delete db;
}

TEST(LevelDBDemo, Atomic) {
  leveldb::DB* db = init_db(get_options());
  db->Put(leveldb::WriteOptions{}, "k1", "v1");
  db->Put(leveldb::WriteOptions{}, "k2", "v2");

  // Batch write
  leveldb::WriteBatch batch;
  batch.Delete("k1");
  batch.Put("k2", "new-v2");
  auto s = db->Write(leveldb::WriteOptions{}, &batch);
  ASSERT_TRUE(s.ok());

  std::string v1;
  s = db->Get(leveldb::ReadOptions(), "k1", &v1);
  ASSERT_TRUE(s.IsNotFound());

  std::string v2;
  s = db->Get(leveldb::ReadOptions(), "k2", &v2);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(v2, "new-v2");

  delete db;
}

TEST(LevelDBDemo, SyncWrite) {
  leveldb::DB* db = init_db(get_options());

  // Sync write
  leveldb::WriteOptions write_options;
  write_options.sync = true;
  auto s = db->Put(write_options, "sync-write-key", "sync-write");
  ASSERT_TRUE(s.ok());

  delete db;
}

TEST(LevelDBDemo, Iteration) {
  leveldb::DB* db = init_db(get_options());
  leveldb::WriteBatch batch;
  for (int i = 0; i < 100; ++i) {
    std::string idx = std::to_string(i);
    batch.Put("iter-key-" + idx, "iter-value-" + idx);
  }
  auto s = db->Write(leveldb::WriteOptions{}, &batch);

  // Iteration
  std::cout << "\n###### Iteration ######\n" << std::endl;
  leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString()
              << std::endl;
  }
  ASSERT_TRUE(it->status().ok());  // Check for any errors found during the scan
  delete it;

  // Reverse Iteration
  std::cout << "\n###### Reverse Iteration ######\n" << std::endl;
  it = db->NewIterator(leveldb::ReadOptions());
  for (it->SeekToLast(); it->Valid(); it->Prev()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString()
              << std::endl;
  }
  ASSERT_TRUE(it->status().ok());  // Check for any errors found during the scan
  delete it;

  // Iterate Range: [start, limit)
  std::cout << "\n###### Iterate Range: [start, limit) ######\n" << std::endl;
  std::string limit = "iter-key-2";
  it = db->NewIterator(leveldb::ReadOptions());
  for (it->SeekToFirst(); it->Valid() && it->key().ToString() < limit;
       it->Next()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString()
              << std::endl;
  }
  ASSERT_TRUE(it->status().ok());  // Check for any errors found during the scan
  delete it;

  // Iterate Range: [seek, limit)
  std::cout << "\n###### Iterate Range: [seek, limit) ######\n" << std::endl;
  std::string seek = "iter-key-2";
  limit = "iter-key-3";
  it = db->NewIterator(leveldb::ReadOptions());
  for (it->Seek(seek); it->Valid() && it->key().ToString() < limit;
       it->Next()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString()
              << std::endl;
  }
  ASSERT_TRUE(it->status().ok());  // Check for any errors found during the scan
  delete it;

  delete db;
}

TEST(LevelDBDemo, Snapshot) {
  leveldb::DB* db = init_db(get_options());
  std::string k = "snapshot-key", v = "snapshot-value";
  db->Put(leveldb::WriteOptions{}, k, v);

  // Create snapshot
  leveldb::ReadOptions snapshot_options;
  snapshot_options.snapshot = db->GetSnapshot();

  // Do some updates
  db->Put(leveldb::WriteOptions{}, k, v + "-updated");

  // Read not using snapshot
  std::string updated_val;
  db->Get(leveldb::ReadOptions{}, k, &updated_val);
  std::cout << "Read with no snapshots: " << updated_val << std::endl;
  ASSERT_EQ(updated_val, v + "-updated");

  // Read using snapshot
  std::string snapshot_read_val;
  db->Get(snapshot_options, k, &snapshot_read_val);
  std::cout << "Read with snapshot: " << snapshot_read_val << std::endl;
  ASSERT_EQ(snapshot_read_val, v);

  // Release snapshot
  db->ReleaseSnapshot(snapshot_options.snapshot);

  delete db;
}

TEST(LevelDBDemo, Comparator) {
  // A Comparator implementation
  class TwoPartComparator : public leveldb::Comparator {
   public:
    // Three-way comparison function:
    //   if a < b: negative result
    //   if a > b: positive result
    //   else: zero result
    int Compare(const leveldb::Slice& a,
                const leveldb::Slice& b) const override {
      long a1, a2, b1, b2;
      ParseKey(a, &a1, &a2);
      ParseKey(b, &b1, &b2);
      if (a1 < b1) return -1;
      if (a1 > b1) return +1;
      if (a2 < b2) return -1;
      if (a2 > b2) return +1;
      return 0;
    }

    const char* Name() const override { return "TwoPartComparator"; }
    void FindShortestSeparator(std::string*,
                               const leveldb::Slice&) const override {}
    void FindShortSuccessor(std::string*) const override {}

   private:
    static void ParseKey(const leveldb::Slice& k, long* x1, long* x2) {
      std::string parts = k.ToString();
      auto index = parts.find_first_of(':');
      *x1 = strtol(parts.substr(0, index).c_str(), nullptr, 10);
      *x2 = strtol(parts.substr(index + 1, parts.size()).c_str(), nullptr, 10);
    }
  };

  leveldb::DB* db;
  leveldb::Options options;
  TwoPartComparator cmp;
  options.create_if_missing = true;
  options.comparator = &cmp;
  leveldb::Status status =
      leveldb::DB::Open(options, "/tmp/comparator-demo", &db);
  ASSERT_TRUE(status.ok());

  // populate the database
  leveldb::Slice key1 = "1:3";
  leveldb::Slice key2 = "2:3";
  leveldb::Slice key3 = "2:1";
  leveldb::Slice key4 = "2:100";
  std::string val1 = "one";
  std::string val2 = "two";
  std::string val3 = "three";
  std::string val4 = "four";
  db->Put(leveldb::WriteOptions(), key1, val1);
  db->Put(leveldb::WriteOptions(), key2, val2);
  db->Put(leveldb::WriteOptions(), key3, val3);
  db->Put(leveldb::WriteOptions(), key4, val4);

  // iterate the database
  leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString()
              << std::endl;
  }
  // 1:3: one
  // 2:1: three
  // 2:3: two
  // 2:100: four
  delete it;

  // Open a wrong comparator database cause error
  status = leveldb::DB::Open(options, db_name, &db);
  ASSERT_FALSE(status.ok());
  std::cout << "Open a wrong comparator database: " << status.ToString()
            << std::endl;
  ASSERT_TRUE(status.IsInvalidArgument());

  delete db;
}

TEST(LevelDBDemo, Filter) {
  class CustomFilterPolicy : public leveldb::FilterPolicy {
   public:
    explicit CustomFilterPolicy(int i)
        : builtin_policy_(leveldb::NewBloomFilterPolicy(i)) {}
    ~CustomFilterPolicy() override { delete builtin_policy_; }

    const char* Name() const override { return "IgnoreTrailingSpacesFilter"; }

    void CreateFilter(const leveldb::Slice* keys, int n,
                      std::string* dst) const override {
      // Use builtin bloom filter code after removing trailing spaces
      std::vector<leveldb::Slice> trimmed(n);
      int i;
      for (i = 0; i < n; i++) {
        trimmed[i] = RemoveTrailingSpaces(keys[i]);
      }
      return builtin_policy_->CreateFilter(&trimmed[i], n, dst);
    }

    bool KeyMayMatch(const leveldb::Slice& key,
                     const leveldb::Slice& filter) const override {
      // Use builtin bloom filter code after removing trailing spaces
      return builtin_policy_->KeyMayMatch(RemoveTrailingSpaces(key), filter);
    }

   private:
    static leveldb::Slice RemoveTrailingSpaces(leveldb::Slice s) {
      std::string str = s.ToString();
      const auto strBegin = str.find_first_not_of(' ');
      if (strBegin == std::string::npos) return "";  // no content

      const auto strEnd = str.find_last_not_of(' ');
      const auto strRange = strEnd - strBegin + 1;

      return str.substr(strBegin, strRange);
    }

   private:
    const FilterPolicy* builtin_policy_;
  };

  leveldb::DB* db;
  leveldb::Options options;
  CustomFilterPolicy filter(100);
  options.create_if_missing = true;
  options.filter_policy = &filter;
  leveldb::Status status = leveldb::DB::Open(options, "/tmp/filter-demo", &db);
  ASSERT_TRUE(status.ok());

  // populate the database
  leveldb::Slice key1 = "hello";
  leveldb::Slice key2 = " hello";
  leveldb::Slice key3 = "hello ";
  leveldb::Slice key4 = " hello ";
  std::string val1 = "one";
  std::string val2 = "two";
  std::string val3 = "three";
  std::string val4 = "four";
  db->Put(leveldb::WriteOptions(), key1, val1);
  db->Put(leveldb::WriteOptions(), key2, val2);
  db->Put(leveldb::WriteOptions(), key3, val3);
  db->Put(leveldb::WriteOptions(), key4, val4);

  // iterate the database
  leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString()
              << std::endl;
  }
  //  hello: two
  // hello : four
  // hello: one
  // hello : three
  delete it;

  delete db;
}

TEST(LevelDBDemo, GetApproximateSizes) {
  leveldb::DB* db = init_db(get_options());

  // GetApproximateSizes
  leveldb::Range ranges[2];
  ranges[0] = leveldb::Range("a", "c");
  ranges[1] = leveldb::Range("x", "z");
  uint64_t sizes[2];
  db->GetApproximateSizes(ranges, 2, sizes);

  std::cout << "sizes[0]: " << sizes[0] << ", sizes[1]: " << sizes[1]
            << std::endl;

  delete db;
}

int main(int argc, char** argv) {
  printf("Running main() from %s\n", __FILE__);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
