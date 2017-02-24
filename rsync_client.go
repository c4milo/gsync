package rsync

import (
	"context"
	"io"

	"github.com/golang/glog"
	"github.com/huichen/murmur"
	"github.com/pkg/errors"
)

// Sync sends file deltas or literals to the caller in order to efficiently re-construct a remote file.
// The caller has to make sure to close the "c" channel after sending all the block checksums or this
// function will deadlock. Once done sending all the block operations, the "o" channel is close to
// signal the caller the end of transsmision.
func Sync(ctx context.Context, r io.Reader, c <-chan BlockChecksum) chan<- BlockOperation {
	// Build lookup table using remote signatures
	t := make(map[uint32][]BlockChecksum)
	for sum := range c {
		if sum.Error != nil {
			// we continue reading just fine and print out a warning. Worst case scenario, the involved
			// data block is re-sent.
			glog.Warningf("block checksum error: %+v", sum.Error)
		}

		k := sum.Weak
		t[k] = append(t[k], sum)
	}

	var index uint64
	buffer := make([]byte, 0, DefaultBlockSize)
	o := make(chan<- BlockOperation)

	go func() {
		defer close(o)
		// Read the file, see if there are content matches against remote blocks and send literal or data operation.
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
				// return since data corruption in the server might be possible.
				return
			}

			block := buffer[:n]
			weak := rollingHash(block)

			op := BlockOperation{Index: index}
			if bs, ok := t[weak]; ok {
				for _, b := range bs {
					if murmur.Murmur3(block) == b.Strong {
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
