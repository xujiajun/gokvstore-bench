package kvstore_bench

import (
	"fmt"
	"os"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/xujiajun/utils/filesystem"
)

var levelDB *leveldb.DB

func init() {
	dir := "testdata/levelDB"
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

	//set level db
	levelDB, err = leveldb.OpenFile(dir, nil)
	if err != nil {
		panic(err)
	}
}

func BenchmarkLevelDBPutValue64B(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		key := getKey(n)
		val := geyValue64B()
		err = levelDB.Put(key, val, nil)
	}
}

func BenchmarkLevelDBPutValue128B(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		key := getKey(n)
		val := geyValue128B()
		err = levelDB.Put(key, val, nil)
	}
}

func BenchmarkLevelDBPutValue256B(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		key := getKey(n)
		val := geyValue256B()
		err = levelDB.Put(key, val, nil)
	}
}

func BenchmarkLevelDBPutValue512B(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		key := getKey(n)
		val := geyValue512B()
		err = levelDB.Put(key, val, nil)
	}
}

func BenchmarkLevelDBGet(b *testing.B) {
	InitLevelDBData()

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		key := []byte("key_" + fmt.Sprintf("%07d", 99))
		_, err := levelDB.Get(key, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func InitLevelDBData() {
	for n := 0; n < 10000; n++ {
		key := getKey(n)
		val := geyValue64B()
		err = levelDB.Put(key, val, nil)
		if err != nil {
			panic(err)
		}
	}
}
