package kvstore_bench

import (
	"fmt"
	"os"
	"testing"

	"github.com/dgraph-io/badger"
	"github.com/xujiajun/utils/filesystem"
)

var badgerDB *badger.DB

func init() {
	dir := "testdata/badgerDB"
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}

	if ok := filesystem.PathIsExist("testdata"); !ok {
		if err := os.Mkdir("testdata", os.ModePerm); err != nil {
			panic(err)
		}

	}

	if ok := filesystem.PathIsExist(dir); !ok {
		if err := os.Mkdir(dir, os.ModePerm); err != nil {
			panic(err)
		}
	}

	// set badgerDB
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir

	badgerDB, err = badger.Open(opts)
	if err != nil {
		panic(err)
	}
}

func InitBadgerDBData() {
	for n := 0; n < 10000; n++ {
		key := getKey(n)
		val := geyValue64B()

		if err = badgerDB.Update(
			func(txn *badger.Txn) error {
				return txn.Set(key, val)
			}); err != nil {
			panic(err)
		}
	}
}

func BenchmarkBadgerDBPutValue64B(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		key := getKey(n)
		val := geyValue64B()

		if err = badgerDB.Update(
			func(txn *badger.Txn) error {
				return txn.Set(key, val)
			}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBadgerDBPutValue128B(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		key := getKey(n)
		val := geyValue128B()

		if err = badgerDB.Update(
			func(txn *badger.Txn) error {
				return txn.Set(key, val)
			}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBadgerDBPutValue256B(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		key := getKey(n)
		val := geyValue256B()

		if err = badgerDB.Update(
			func(txn *badger.Txn) error {
				return txn.Set(key, val)
			}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBadgerDBPutValue512B(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		key := getKey(n)
		val := geyValue512B()

		if err = badgerDB.Update(
			func(txn *badger.Txn) error {
				return txn.Set(key, val)
			}); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkBadgerDBGet(b *testing.B) {
	InitBadgerDBData()

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		err = badgerDB.View(func(txn *badger.Txn) error {
			key := []byte("key_" + fmt.Sprintf("%07d", 99))
			_, err := txn.Get(key)
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
