package kvstore_bench

import (
	"fmt"
	"os"
	"testing"

	"github.com/tidwall/buntdb"
	"github.com/xujiajun/utils/filesystem"
)

var buntDB *buntdb.DB

func init() {
	dir := "testdata/buntDB"
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

	buntDB, err = buntdb.Open(dir + "/buntDB.db")
	if err != nil {
		panic(err)
	}
}

func BenchmarkBuntDBPutValue64B(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		key := string(getKey(n))
		val := string(geyValue64B())

		if err = buntDB.Update(func(tx *buntdb.Tx) error {
			_, _, err := tx.Set(key, val, nil)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuntDBPutValue128B(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		key := string(getKey(n))
		val := string(geyValue128B())

		if err = buntDB.Update(func(tx *buntdb.Tx) error {
			_, _, err := tx.Set(key, val, nil)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuntDBPutValue256B(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		key := string(getKey(n))
		val := string(geyValue256B())
		if err = buntDB.Update(func(tx *buntdb.Tx) error {
			_, _, err := tx.Set(key, val, nil)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuntDBPutValue512B(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		key := string(getKey(n))
		val := string(geyValue512B())
		if err = buntDB.Update(func(tx *buntdb.Tx) error {
			_, _, err := tx.Set(key, val, nil)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuntDBGet(b *testing.B) {
	InitBuntDBData()

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		err = buntDB.View(func(tx *buntdb.Tx) error {
			key := "key_" + fmt.Sprintf("%07d", 99)
			_, err := tx.Get(key)
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

func InitBuntDBData() {
	for n := 0; n < 10000; n++ {
		key := string(getKey(n))
		val := string(geyValue64B())

		if err = buntDB.Update(func(tx *buntdb.Tx) error {
			_, _, err := tx.Set(key, val, nil)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			panic(err)
		}
	}

}
