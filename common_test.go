package inverted_index_2

import "os"

func makeTmpDir() string {
	dir, err := os.MkdirTemp("", "")
	if err != nil {
		panic(err)
	}
	return dir
}
