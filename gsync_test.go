package gsync

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"hash"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"

	"github.com/hooklift/assert"
)

var alpha = "abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789\n"

// srand generates a random string of fixed size.
func srand(seed int64, size int) []byte {
	buf := make([]byte, size)
	rand.Seed(seed)
	for i := 0; i < size; i++ {
		buf[i] = alpha[rand.Intn(len(alpha))]
	}
	return buf
}

func TestSync(t *testing.T) {
	tests := []struct {
		desc   string
		source []byte
		cache  []byte
		sum    hash.Hash
	}{
		{
			"full sync, no cache",
			srand(10, 512),
			nil,
			md5.New(),
		},
		{
			"partial sync, some cache",
			srand(20, 1024),
			srand(20, 512),
			md5.New(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if len(tt.cache) > 0 {
				assert.Equals(t, tt.source[:len(tt.cache)], tt.cache)
			}

			// fmt.Printf("source len: %d bytes\n", len(tt.source))
			// fmt.Printf("cache len: %d bytes\n", len(tt.cache))

			fmt.Print("Checksum... ")
			sumsCh, err := Checksums(ctx, bytes.NewReader(tt.cache), tt.sum)
			assert.Ok(t, err)
			fmt.Println("done")

			fmt.Print("LookUpTable... ")
			cacheSums, err := LookUpTable(ctx, sumsCh)
			assert.Ok(t, err)
			fmt.Printf("%d blocks. done\n", len(cacheSums))

			fmt.Print("Sync... ")
			opsCh, err := Sync(ctx, bytes.NewReader(tt.source), tt.sum, cacheSums)
			assert.Ok(t, err)
			fmt.Println("done")

			fmt.Print("Apply... ")
			target := new(bytes.Buffer)
			err = Apply(ctx, target, bytes.NewReader(tt.cache), opsCh)
			assert.Ok(t, err)
			fmt.Println("done")

			assert.Cond(t, target.Len() != 0, "target file should not be empty")
			assert.Equals(t, len(tt.source), target.Len())
			if !bytes.Equal(tt.source, target.Bytes()) {
				ioutil.WriteFile("source.txt", tt.source, 0640)
				ioutil.WriteFile("cache.txt", tt.cache, 0640)
				ioutil.WriteFile("target.txt", target.Bytes(), 0640)
			}
			assert.Cond(t, bytes.Equal(tt.source, target.Bytes()), "source and target files are different")
		})
	}
}

// Benchmarks using buffered channels.
func BenchmarkBufferedChannel(b *testing.B) {}

// Benchmarks using unbuffered channels.
func BenchmarkUnbufferedChannel(b *testing.B) {}

func Benchmark6kbBlockSize(b *testing.B)    {}
func Benchmark128kbBlockSize(b *testing.B)  {}
func Benchmark512kbBlockSize(b *testing.B)  {}
func Benchmark1024kbBlockSize(b *testing.B) {}

func BenchmarkMD5(b *testing.B)     {}
func BenchmarkSHA256(b *testing.B)  {}
func BenchmarkSHA512(b *testing.B)  {}
func BenchmarkMurmur3(b *testing.B) {}
func BenchmarkXXHash(b *testing.B)  {}
