package rsync

import (
	"context"
	"hash"
	"io"

	"github.com/pkg/errors"
)

// Checksums reads data blocks from reader and pipes out block checksums on the
// returning channel, closing it when done reading or when the context is cancelled.
func Checksums(ctx context.Context, r io.Reader, shash hash.Hash) chan<- BlockChecksum {
	var index uint64
	buffer := make([]byte, 0, DefaultBlockSize)
	c := make(chan<- BlockChecksum)

	go func() {
		defer close(c)

		for {
			// Allow for cancellation
			select {
			case <-ctx.Done():
				c <- BlockChecksum{Error: ctx.Err()}
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
				c <- BlockChecksum{Error: errors.Wrapf(err, "failed reading file")}
				// let the caller decide whether to interrupt the process or not.
				continue
			}

			block := buffer[:n]
			weak := rollingHash(block)
			strong := shash.Sum(block)

			c <- BlockChecksum{
				Index:  index,
				Weak:   weak,
				Strong: strong,
			}
			index++
		}
	}()

	return c
}

// Apply reconstructs a file given a set of operations. The caller must close the ops channel or the context when done or there willl be a deadlock.
func Apply(ctx context.Context, dst io.WriterAt, cache io.ReaderAt, ops <-chan BlockOperation) error {
	buffer := make([]byte, 0, DefaultBlockSize)

	for o := range ops {
		// Allow for cancellation.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// break out of the select block and continue reading ops
			break
		}

		var block []byte
		index := int64(o.Index)
		indexB := int64(o.IndexB)

		if len(o.Data) > 0 {
			block = o.Data
		} else {
			n, err := cache.ReadAt(buffer, indexB)
			if err != nil && err != io.EOF {
				return errors.Wrapf(err, "failed reading cached data")
			}

			block = buffer[:n]
		}

		_, err := dst.WriteAt(block, index)
		if err != nil {
			return errors.Wrapf(err, "failed writing block at %d", o.Index)
		}
	}
	return nil
}
