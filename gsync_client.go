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
	var currentOffset, lastMatchOffset int

	// newData determines whether there were local changes that need to be synced up with the server.
	newData := false
	// fullRHash determines whether a rolling checksum from an entire block must be done.
	fullRHash := true

	if r == nil {
		return nil, errors.New("gsync: reader required")
	}

	o := make(chan BlockOperation)

	if shash == nil {
		shash = sha256.New()
	}

	go func() {
		index := 1

		defer func() {
			close(o)
			fmt.Printf("Blocks restored from cache: %d\n", index)
		}()

		// Initializes variables to incrementally calculate rolling hash
		var r1, r2, rhash, lastByteOffsetValue uint32
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
			n, err := r.ReadAt(buffer, int64(currentOffset))
			if err != nil && err != io.EOF {
				o <- BlockOperation{
					Error: errors.Wrapf(err, "failed reading data block"),
				}
				// return since data corruption in the server is possible and a re-sync is required.
				return
			}

			block := buffer[:n]
			if len(remote) == 0 {
				o <- BlockOperation{Data: block}
				currentOffset += n

				if err == io.EOF {
					return
				}
				continue
			}

			if fullRHash {
				r1, r2, rhash = rollingHash(block)
				fullRHash = false
			} else {
				r1, r2, rhash = rollingHash2(block, r1, r2, lastByteOffsetValue)
			}

			if bs, ok := remote[rhash]; ok {
				shash.Reset()
				shash.Write(block)
				s := shash.Sum(nil)

				matchFound := false

				for _, b := range bs {
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

					if !bytes.Equal(s, b.Strong) {
						continue
					}

					matchFound = true
					fullRHash = true

					// When a match is found, sends the data between the current file
					// offset and the end of the previous match
					if newData {
						sendData(ctx, r, o, currentOffset, lastMatchOffset)
						newData = false
					}

					// If a match is found, the search is restarted at the end of the matched block.
					currentOffset += len(block)

					// Keep track of the last match offset
					lastMatchOffset = currentOffset

					// Keep track of the last byte offset value for use in incremental rolling checksum
					lastByteOffsetValue = uint32(block[0])

					// instructs the remote end to copy block data at offset b.Index
					// from remote file.
					o <- BlockOperation{Index: b.Index}
					index++

					break
				} // ends for-loop for 2nd level signature search

				if !matchFound {
					newData = true
					lastByteOffsetValue = uint32(block[0])
					currentOffset++
				}
			} else {
				newData = true
				lastByteOffsetValue = uint32(block[0])
				currentOffset++
			}

			if err == io.EOF {
				if newData {
					sendData(ctx, r, o, currentOffset, lastMatchOffset)
				}
				return
			}
		} // ends main for-loop reading the file
	}()

	return o, nil
}

func sendData(ctx context.Context, r io.ReaderAt, o chan<- BlockOperation, currentOffset, lastMatchOffset int) {
	offset := lastMatchOffset
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
		n, err := r.ReadAt(buffer, int64(offset))
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

		offset += n
	}
}
