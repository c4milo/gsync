// This Source Code Form is subject to the terms of the Mozilla Public
// License, version 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package gsync

import (
	"context"
	"crypto/sha256"
	"hash"
	"io"

	"github.com/pkg/errors"
)

// Signatures reads data blocks from reader and pipes out block signatures on the
// returning channel, closing it when done reading or when the context is cancelled.
// This function does not block and returns immediately. The caller must make sure the concrete
// reader instance is not nil or this function will panic.
func Signatures(ctx context.Context, r io.Reader, shash hash.Hash) (<-chan BlockSignature, error) {
	var index uint64
	c := make(chan BlockSignature)

	bfp := bufferPool.Get().(*[]byte)
	buffer := *bfp
	defer bufferPool.Put(bfp)

	if r == nil {
		close(c)
		return nil, errors.New("gsync: reader required")
	}

	if shash == nil {
		shash = sha256.New()
	}

	go func() {
		defer close(c)

		for {
			// Allow for cancellation
			select {
			case <-ctx.Done():
				c <- BlockSignature{
					Index: index,
					Error: ctx.Err(),
				}
				return
			default:
				// break out of the select block and continue reading
				break
			}

			n, err := r.Read(buffer)
			if err == io.EOF {
				break
			}

			if err != nil {
				c <- BlockSignature{
					Index: index,
					Error: errors.Wrapf(err, "failed reading block"),
				}
				index++
				// let the caller decide whether to interrupt the process or not.
				continue
			}

			block := buffer[:n]
			shash.Reset()
			shash.Write(block)
			strong := shash.Sum(nil)
			_, _, rhash := rollingHash(block)

			c <- BlockSignature{
				Index:  index,
				Weak:   rhash,
				Strong: strong,
			}
			index++
		}
	}()

	return c, nil
}

// Apply reconstructs a file given a set of operations. The caller must close the ops channel or the context when done or there will be a deadlock.
func Apply(ctx context.Context, dst io.Writer, cache io.ReaderAt, ops <-chan BlockOperation) error {
	bfp := bufferPool.Get().(*[]byte)
	buffer := *bfp
	defer bufferPool.Put(bfp)

	for o := range ops {
		// Allows for cancellation.
		select {
		case <-ctx.Done():
			return errors.Wrapf(ctx.Err(), "failed applying block operations")
		default:
			// break out of the select block and continue reading ops
			break
		}

		if o.Error != nil {
			return errors.Wrapf(o.Error, "failed applying operation")
		}

		var block []byte

		if len(o.Data) > 0 {
			block = o.Data
		} else {
			index := int64(o.Index)
			n, err := cache.ReadAt(buffer, (index * DefaultBlockSize))
			if err != nil && err != io.EOF {
				return errors.Wrapf(err, "failed reading cached block")
			}

			block = buffer[:n]
		}

		_, err := dst.Write(block)
		if err != nil {
			return errors.Wrapf(err, "failed writing block to destination")
		}
	}
	return nil
}
