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
func Sync(ctx context.Context, r io.ReaderAt, shash hash.Hash, remote map[uint32][]BlockSignature) (<-chan BlockOperation, error) {
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
			offset             int64
			rolling, match     bool
		)

		delta := make([]byte, 0)

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

			buffer := make([]byte, DefaultBlockSize)

			n, err := r.ReadAt(buffer, offset)
			if err != nil && err != io.EOF {
				o <- BlockOperation{
					Error: errors.Wrapf(err, "failed reading data block"),
				}
				// return since data corruption in the server is possible and a re-sync is required.
				return
			}

			block := buffer[:n]

			// If there are no block signatures from remote server, send all data blocks
			if len(remote) == 0 {
				o <- BlockOperation{Data: block}
				offset += int64(n)

				if err == io.EOF {
					return
				}
				continue
			}

			if rolling {
				new := uint32(block[n-1])
				r1, r2, rhash = rollingHash2(uint32(n), r1, r2, old, new)
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

					if len(delta) > 0 {
						send(ctx, bytes.NewReader(delta), o)
						delta = make([]byte, 0)
					}

					// instructs the remote end to copy block data at offset b.Index
					// from remote file.
					o <- BlockOperation{Index: b.Index}
					break
				}
			}

			if match {
				if err == io.EOF {
					break
				}
				match = false
				old, rhash, r1, r2, rolling = 0, 0, 0, 0, false
				offset += int64(n)
			} else {
				// If EOF is reached and we didn't get a block hash match, we copy all read data into
				// delta slice in order to not lose the deltas at the end of the file.
				if err == io.EOF {
					for _, k := range block {
						delta = append(delta, k)
					}

					if len(delta) > 0 {
						send(ctx, bytes.NewReader(delta), o)
					}

					break
				}
				rolling = true
				old = uint32(block[0])
				delta = append(delta, block[0])
				offset++
			}
		}
	}()

	return o, nil
}

// send sends all deltas over the channel. Any error is reported back using the
// same channel.
func send(ctx context.Context, r io.Reader, o chan<- BlockOperation) {
	for {
		// Allow for cancellation.
		select {
		case <-ctx.Done():
			o <- BlockOperation{
				Error: ctx.Err(),
			}
			return
		default:
			// break out of the select block and continue reading
			break
		}

		buffer := make([]byte, DefaultBlockSize)

		n, err := r.Read(buffer)
		if err != nil && err != io.EOF {
			o <- BlockOperation{
				Error: errors.Wrapf(err, "failed reading data block"),
			}
			return
		}

		o <- BlockOperation{Data: buffer[:n]}

		if err == io.EOF {
			break
		}
	}
}
