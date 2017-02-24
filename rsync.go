package rsync

const (
	// DefaultBlockSize is the default block size.
	DefaultBlockSize = 1024 * 6
)

// Rolling checksum is up to 16 bit length for simplicity and speed.
const (
	mod = 1 << 16
)

// rollingHash as defined in https://www.samba.org/~tridge/phd_thesis.pdf
func rollingHash(block []byte) uint32 {
	var a, b uint32
	l := len(block) - 1
	for i, k := range block {
		a += uint32(k)
		b += (uint32(l) - uint32(i) + 1) * uint32(k)
	}
	r1 := a % mod
	r2 := b % mod
	r := r1 + (mod * r2)

	return r
}

// BlockChecksum contains file block checksums as specified in rsync thesis.
type BlockChecksum struct {
	// Index is the block index
	Index uint64
	// Strong refers to the expensive checksum, for our implementation it is murmur3.
	Strong []byte
	// Weak refers to the fast rsync rolling checksum
	Weak uint32
	// Error is used to report the error reading the file or calculating checksums.
	Error error
}

// BlockOperation represents a file re-construction instruction.
type BlockOperation struct {
	// Index is the block index in the source file.
	Index uint64
	// IndexB is the block index to copy from the remote file, avoiding network transmission.
	IndexB uint64
	// Data is the delta to be applied to the remote file. No data means
	// the client found a matching checksum for this block, which means that the remote end proceeds to
	// copy the block data from its local copy instead.
	Data []byte
	// Error is used to report any error while sending operations.
	Error error
}
