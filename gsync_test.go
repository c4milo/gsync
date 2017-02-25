package gsync

import "testing"

func TestServerChecksum(t *testing.T) {

}

func TestServerApply(t *testing.T) {

}

func TestClientSync(t *testing.T) {

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
