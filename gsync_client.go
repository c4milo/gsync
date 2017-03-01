// This Source Code Form is subject to the terms of the Mozilla Public
// License, version 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package gsync

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"hash"
	"io"

	"github.com/pkg/errors"
)

// LookUpTable returns a map with all the block checksums returned by the remote server for a given file being synchronized.
func LookUpTable(ctx context.Context, bc <-chan BlockChecksum) (map[uint32][]BlockChecksum, error) {
	table := make(map[uint32][]BlockChecksum)
	for c := range bc {
		select {
		case <-ctx.Done():
			return table, errors.Wrapf(ctx.Err(), "failed building lookup table")
		default:
			break
		}

		if c.Error != nil {
			fmt.Printf("warn: checksum error: %#v\n", c.Error)
			continue
		}
		table[c.Weak] = append(table[c.Weak], c)
	}

	return table, nil
}

// Sync sends file deltas or literals to the caller in order to efficiently re-construct a remote file. Whether to send
// data or literals is determined by the remote checksums provided by the caller.
// This function does not block and returns immediately. Also, the remote map is accessed without a mutex.
// The caller must make sure the concrete reader instance is not nil or this function will panic.
func Sync(ctx context.Context, r io.Reader, shash hash.Hash, remote map[uint32][]BlockChecksum) (<-chan BlockOperation, error) {
	var index uint64
	o := make(chan BlockOperation)

	if r == nil {
		close(o)
		return nil, errors.New("gsync: reader required")
	}

	if shash == nil {
		shash = md5.New()
	}

	go func() {
		defer close(o)
		// Read the file, see if there are content matches against remote blocks and send literal or data operation in order to help to reconstruct
		// the file in the remote end.
		for {

			// Allow for cancellation.
			select {
			case <-ctx.Done():
				o <- BlockOperation{
					Index: index,
					Error: ctx.Err(),
				}
				return
			default:
				// break out of the select block and continue reading
				break
			}

			buffer := make([]byte, DefaultBlockSize)
			n, err := r.Read(buffer)
			if err == io.EOF {
				break
			}

			// fmt.Printf("%d bytes read from source: %#v\n", n, err)
			// fmt.Printf("len(p): %d\n", len(buffer))

			if err != nil {
				o <- BlockOperation{
					Index: index,
					Error: errors.Wrapf(err, "failed reading block"),
				}
				// return since data corruption in the server is possible and a re-sync is required.
				return
			}

			block := buffer[:n]
			weak := rollingHash(block)

			op := BlockOperation{Index: index}
			if bs, ok := remote[weak]; ok {
				for _, b := range bs {
					shash.Reset()
					if bytes.Equal(shash.Sum(block), b.Strong) {
						// instructs the remote end to copy block data at offset b.Index
						// from remote file.
						op.Index = b.Index
						break
					}
				}
			} else {
				//fmt.Printf("\nsync: %s\n", block)
				op.Data = block
			}

			o <- op
			index++
		}
	}()

	return o, nil
}
