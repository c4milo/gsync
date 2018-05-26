// This Source Code Form is subject to the terms of the Mozilla Public
// License, version 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package gsync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"

	"github.com/pkg/errors"
)

// LookUpTable reads up blocks signatures and builds a lookup table for the client to search from when trying to decide
// wether to send or not a block of data.
func LookUpTable(ctx context.Context, bc <-chan BlockSignature) (map[uint32][]BlockSignature, error) {
	table := make(map[uint32][]BlockSignature)
	for c := range bc {
		select {
		case <-ctx.Done():
			return table, errors.Wrapf(ctx.Err(), "failed building lookup table")
		default:
			break
		}

		if c.Error != nil {
			fmt.Printf("gsync: checksum error: %#v\n", c.Error)
			continue
		}
		table[c.Weak] = append(table[c.Weak], c)
	}

	return table, nil
}

// Sync sends tokens or literal bytes to the caller in order to efficiently re-construct a remote file. Whether to send
// tokens or literals is determined by the remote checksums provided by the caller.
// This function does not block and returns immediately. Also, the remote blocks map is accessed without a mutex,
// so this function is expected to be called once the remote blocks map is fully populated.
//
// The caller must make sure the concrete reader instance is not nil or this function will panic.
func Sync(ctx context.Context, r io.Reader, shash hash.Hash, remote map[uint32][]BlockSignature) (<-chan BlockOperation, error) {
	if r == nil {
		return nil, errors.New("gsync: reader required")
	}

	o := make(chan BlockOperation)

	if shash == nil {
		shash = sha256.New()
	}

	go func() {

		var (
			r1, r2, rhash, old uint32
			offset, max, delta int
			rolling, match     bool
			err                error
		)
		bufferSize := 16 * BlockSize
		buffer := make([]byte, bufferSize)

		defer func() {
			close(o)
		}()

		for {
			// Allow for cancellation.
			select {
			case <-ctx.Done():
				o <- BlockOperation{
					Error: ctx.Err(),
				}
				return
			default:
				break
			}

			for offset > max-BlockSize {
				if err == io.EOF {
					// If EOF is reached and not match data found, we add trailing data
					// to delta array.
					left := max - offset + delta
					for left >= BlockSize {
						o <- BlockOperation{Data: append([]byte(nil), buffer[max-left:max-left+BlockSize]...)}
						left -= BlockSize

					}
					if left > 0 {
						o <- BlockOperation{Data: append([]byte(nil), buffer[max-left:max]...)}
					}
					return
				}

				var n int
				left := copy(buffer[:], buffer[offset-delta:max])
				n, err = r.Read(buffer[left:])
				if err != nil && err != io.EOF {
					o <- BlockOperation{
						Error: errors.Wrapf(err, "failed reading data block"),
					}
					// return since data corruption in the server is possible and a re-sync is required.
					return
				}
				offset = delta
				max = left + n
			}
			// If there are no block signatures from remote server, send all data blocks
			if len(remote) == 0 {
				for max-offset >= BlockSize {
					o <- BlockOperation{Data: append([]byte(nil), buffer[offset:offset+BlockSize]...)}
					offset += BlockSize
				}
				continue
			}

			left := BlockSize
			if max-offset < BlockSize {
				// FIXME: is this called?
				left = max - offset
			}
			block := buffer[offset:offset+left]
			if rolling {
				new := uint32(buffer[offset+left-1])
				r1, r2, rhash = rollingHash2(uint32(left), r1, r2, old, new)
			} else {
				r1, r2, rhash = rollingHash(block)
			}

			if bs, ok := remote[rhash]; ok {
				shash.Reset()
				shash.Write(block)
				s := shash.Sum(nil)

				for _, b := range bs {
					if !bytes.Equal(s, b.Strong) {
						continue
					}

					match = true

					// We need to send deltas before sending an index token.
					if delta > 0 {
						o <- BlockOperation{Data: append([]byte(nil), buffer[offset-delta:offset]...)}
						delta = 0
					}

					// instructs the server to copy block data at offset b.Index
					// from its own copy of the file.
					o <- BlockOperation{Index: b.Index}
					break
				}
			}

			if match {
				rolling, match = false, false
				old, rhash, r1, r2 = 0, 0, 0, 0
				offset += left
			} else {
				rolling = true
				old = uint32(buffer[offset])
				delta++
				offset++
				if delta >= BlockSize {
					o <- BlockOperation{Data: append([]byte(nil), buffer[offset-delta:offset-delta+BlockSize]...)}
					delta -= BlockSize
				}
			}
		}
	}()

	return o, nil
}
