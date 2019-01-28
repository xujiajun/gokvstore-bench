package kvstore_bench

import "fmt"

func getKey(n int) []byte {
	return []byte("key_" + fmt.Sprintf("%07d", n))
}

func geyValue64B() []byte {
	return []byte("valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalv")
}

func geyValue128B() []byte {
	return []byte("valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvval" +
		"valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalv")
}

func geyValue256B() []byte {
	return []byte("valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvvalvalvalvalvalvalvalvalvalvalval" +
		"valvalvalvalvalvalvalvalvalvalvvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvvalvalvalval" +
		"valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalv")
}

func geyValue512B() []byte {
	return []byte("valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvvalvalvalvalvalvalvalvalvalvalval" +
		"valvalvalvalvalvalvalvalvalvalvvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvvalvalvalval" +
		"valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalv" + "valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvvalvalvalvalvalvalvalvalvalvalval" +
		"valvalvalvalvalvalvalvalvalvalvvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvvalvalvalval" +
		"valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalv")
}
