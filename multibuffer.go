// multibuffer is a thread-safe single-writer multiple-reader buffer
// It provides an io.Writer which writes to an internal buffer.
// Readers can then read out that data.
// Access is internally synchronized, so no external synchronization is needed.
package multibuffer

import (
	"bytes"
	"fmt"
	"io"
	"sync"
)

// New MultiBuffer returns a writer, and the MultiBuffer that writer writes into.
func New() (io.WriteCloser, *MultiBuffer) {
	mb := &MultiBuffer{}

	// Initialize the condition variable with a read-locker from the rwMutex
	// We need to pass RLocker because the waiters are readers, who hold a read
	// lock.
	mb.cond = sync.NewCond(mb.rwMutex.RLocker())

	return &writer{mb: mb}, mb
}

// MultiBuffer wraps a bytes.Buffer with a sync.RWMutex.
// Data-race safety:  buffer must only be modified when the rwMutex is held for
// writing, and only read  when held for reading.  In this code, writing is
// handled by passing the Write directly to the underlying bytes.Buffer, which
// is done under the write lock.  Reading is done by copying data from the
// .Bytes() method which exposes a slice from the buffer.  Care must be taken
// not to keep any references to that slice outside of the read lock, as it
// will be written to.
type MultiBuffer struct {
	// rwMutex must be used for any field access in this struct
	rwMutex sync.RWMutex

	// This condition variable is used to announce via broadcast when new data
	// is written.  This allows readers to wait on it.
	cond *sync.Cond

	// buffer is used in an append-only manner here, since a new reader can
	// come at any point in time.
	//
	// As a potential future optimization, this buffer could be replaced with
	// 2 or more buffers so that readers can process old data with less
	// contention with the writer.
	buffer bytes.Buffer

	// closed is true once the writer has finished writing data and called Close
	closed bool
}

type writer struct {
	mb *MultiBuffer
}

func (w *writer) Write(p []byte) (int, error) {
	// 0 byte writes are handled trivially.
	// Return early to avoid locking or broadcasting, as there's no new data.
	if len(p) == 0 {
		return 0, nil
	}

	n, err := w.syncWrite(p)
	if err != nil {
		return n, err
	}

	// Notify any waiting readers of new data
	w.mb.cond.Broadcast()

	return n, nil
}

// syncWrite locks for writing and writes into the internal buffer
func (w *writer) syncWrite(p []byte) (int, error) {
	w.mb.rwMutex.Lock()
	defer w.mb.rwMutex.Unlock()

	if w.mb.closed {
		return 0, fmt.Errorf("cannot write to already-closed writer")
	}

	// calling Write on buffer while write-lock is held
	return w.mb.buffer.Write(p)
}

func (w *writer) Close() error {
	w.mb.rwMutex.Lock()
	w.mb.closed = true
	w.mb.rwMutex.Unlock()

	// Notify any waiting readers that we are closed
	w.mb.cond.Broadcast()
	return nil
}

// Reader provides an io.Reader which reads the data in this MultiBuffer.
// Each individual reader is not thread-safe, but provides thread-safe access
// to the data in the MultiBuffer.
func (mb *MultiBuffer) Reader() io.ReadSeeker {
	return &reader{mb: mb}
}

func (mb *MultiBuffer) Len() int {
	mb.rwMutex.RLock()
	defer mb.rwMutex.RUnlock()
	// accessing buffer length under read lock
	return mb.buffer.Len()
}

// reader is the concrete type returned by MultiBuffer.Reader
type reader struct {
	mb     *MultiBuffer
	offset int
}

func (r *reader) Seek(offset int64, whence int) (int64, error) {
	currentLen := int64(r.mb.Len())

	switch whence {
	case io.SeekStart:
		// offset is relative to start of file, same as r.offset
	case io.SeekCurrent:
		offset += int64(r.offset)
	case io.SeekEnd:
		offset += currentLen
	default:
		return 0, fmt.Errorf("unknown whence %d", whence)
	}

	if offset < 0 {
		return 0, fmt.Errorf("negative offset after seeking: %d", offset)
	}
	if offset > currentLen {
		// Seeking beyond the end of file is not an error according to the Seek interface.
		// TODO: If we want to support this, Read() will need to be updated to handle invalid offsets
		return 0, fmt.Errorf("seeked beyond current end of file: %d > %d", offset, currentLen)
	}

	// potential narrowing cast is safe because offset is less than the buffer
	// length, which is an int.
	r.offset = int(offset)

	return offset, nil
}

// Read from the multibuffer.
func (r *reader) Read(p []byte) (n int, err error) {
	r.mb.rwMutex.RLock()
	defer r.mb.rwMutex.RUnlock()

	// buf may only be read while locked
	buf := r.mb.buffer.Bytes()

	// No data to read
	for len(buf) == r.offset {
		if r.mb.closed {
			// No data left to read and the writer has closed, so we're at EOF
			return 0, io.EOF
		}

		// Avoid waiting for zero-byte reads
		if len(p) == 0 {
			return 0, nil
		}

		// Wait for a writer to broadcast
		r.mb.cond.Wait()
		// buf is invalid at this point; re-acquire the slice from buffer
		buf = r.mb.buffer.Bytes()
	}

	n = copy(p, buf[r.offset:])
	r.offset += n

	return n, nil
}
