// This Source Code Form is subject to the terms of the Mozilla Public
// License, version 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package gsync implements a rsync-based algorithm for sending delta updates to a remote server.
package gsync

const (
	// DefaultBlockSize is the default block size.
	DefaultBlockSize = 6 * 1024 // 6kb
)

// Rolling checksum is up to 16 bit length for simplicity and speed.
const (
	mod = 1 << 16
)

// rollingHash as defined in https://www.samba.org/~tridge/phd_thesis.pdf, based on Adler-32
func rollingHash(block []byte) (uint32, uint32, uint32) {
	var a, b uint32
	l := uint32(len(block) - 1)
	for i, k := range block {
		a += uint32(k)
		b += (l - uint32(i)) * uint32(k)
	}
	r1 := a % mod
	r2 := b % mod
	r := r1 + (mod * r2)

	return r1, r2, r
}

func rollingHash2(block []byte, r1, r2, old uint32) (uint32, uint32, uint32) {
	l := uint32(len(block))
	new := uint32(block[l-1])

	r1 = (r1 - old + new) % mod
	r2 = (r2 - (l * old) + r1) % mod
	r := r1 + (mod * r2)

	return r1, r2, r
}

// BlockSignature contains file block index and checksums.
type BlockSignature struct {
	// Index is the block index
	Index uint64
	// Strong refers to the strong checksum, it need not to be cryptographic.
	Strong []byte
	// Weak refers to the fast rsync rolling checksum
	Weak uint32
	// Error is used to report the error reading the file or calculating checksums.
	Error error
}

// BlockOperation represents a file re-construction instruction.
type BlockOperation struct {
	// Index is the block index involved.
	Index uint64
	// Data is the delta to be applied to the remote file. No data means
	// the client found a matching checksum for this block, which means that the remote end proceeds to
	// copy the block data from its local copy instead.
	Data []byte
	// Error is used to report any error while sending operations.
	Error error
}
