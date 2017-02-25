package gsync

import (
	"bytes"
	"context"
	"hash"
	"io"

	"github.com/pkg/errors"
)

// Sync sends file deltas or literals to the caller in order to efficiently re-construct a remote file. Whether to send
// data or literals is determined by the remote checksums provided by the caller.
// This function does not block and returns immediately.
func Sync(ctx context.Context, r io.Reader, shash hash.Hash, remote map[uint32][]BlockChecksum) chan<- BlockOperation {
	var index uint64
	buffer := make([]byte, 0, DefaultBlockSize)
	o := make(chan<- BlockOperation)

	go func() {
		defer close(o)
		// Read the file, see if there are content matches against remote blocks and send literal or data operation in order to help to reconstruct
		// the file in the remote end.
		for {
			// Allow for cancellation.
			select {
			case <-ctx.Done():
				o <- BlockOperation{Error: ctx.Err()}
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
				o <- BlockOperation{Error: errors.Wrapf(err, "failed reading file")}
				// return since data corruption in the server is possible and a re-sync is required.
				return
			}

			block := buffer[:n]
			weak := rollingHash(block)

			op := BlockOperation{Index: index}
			if bs, ok := remote[weak]; ok {
				for _, b := range bs {
					if bytes.Compare(shash.Sum(block), b.Strong) == 0 {
						// instructs the remote end to copy block data at offset b.Index
						// from remote file.
						op.IndexB = b.Index
					}
				}
			} else {
				op.Data = block
			}

			o <- op
			index++
		}
	}()

	return o
}
