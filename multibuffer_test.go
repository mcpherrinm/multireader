package multibuffer_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	mathrand "math/rand"
	"testing"

	"github.com/mcpherrinm/jobs/pkg/multibuffer"
	"github.com/stretchr/testify/require"
)

func randBytes(t *testing.T, count int) []byte {
	data := make([]byte, count)
	_, err := rand.Read(data)
	require.NoError(t, err)
	return data
}

func requireRead(t *testing.T, data []byte, reader io.Reader) {
	buf := make([]byte, len(data))
	n, err := reader.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, buf)
}

func requireWrite(t *testing.T, data []byte, writer io.Writer) {
	n, err := writer.Write(data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
}

func requireSeek(t *testing.T, offset int64, whence int, expected int64, seeker io.Seeker) {
	where, err := seeker.Seek(offset, whence)
	require.NoError(t, err)
	require.EqualValues(t, expected, where)
}

// TestWriteCloseRead is the simple no-contention case:
// Write data to buffer then close
// Multiple readers after-the-fact
func TestWriteCloseRead(t *testing.T) {
	count := 10000
	data := randBytes(t, count)

	writer, mb := multibuffer.New()

	// Create a reader before and after writing
	beforeReader := mb.Reader()

	requireWrite(t, data, writer)
	require.NoError(t, writer.Close())

	afterReader := mb.Reader()

	beforeData, err := ioutil.ReadAll(beforeReader)
	require.NoError(t, err)
	afterData, err := ioutil.ReadAll(afterReader)
	require.NoError(t, err)

	require.Equal(t, data, beforeData)
	require.Equal(t, data, afterData)
}

// TestInterlacedNonblocking does a series of different-sized writes followed by a read of the same size.
// The readers should be able to follow without blocking.
func TestInterlacedNonblocking(t *testing.T) {
	writer, mb := multibuffer.New()
	readers := []io.Reader{mb.Reader(), mb.Reader(), mb.Reader()}

	for i := 0; i < 5000; i++ {
		data := randBytes(t, i)
		n, err := writer.Write(data)
		require.NoError(t, err)
		require.Equal(t, i, n)

		for _, reader := range readers {
			requireRead(t, data, reader)
		}
	}

	require.NoError(t, writer.Close())
	for _, reader := range readers {
		buf := make([]byte, 100)
		n, err := reader.Read(buf)
		require.Equal(t, io.EOF, err)
		require.Zero(t, n)
	}
}

// TestInterlacedBlocking sets up a few readers that try to read, might block, and then we write.
// Then we wait on all reads to complete.  Finally, we set the readers to read, and Close() the writer,
// verifying they unblock and get an io.EOF.
func TestInterlacedBlocking(t *testing.T) {
	writer, mb := multibuffer.New()
	readers := []io.Reader{mb.Reader(), mb.Reader(), mb.Reader()}

	for i := 0; i < 5000; i++ {
		data := randBytes(t, i)
		errChan := make(chan error)
		for _, reader := range readers {
			go func(reader io.Reader, data []byte, errChan chan error) {
				buf := make([]byte, len(data))
				n, err := reader.Read(buf)
				if n == len(data) && err == nil {
					errChan <- nil
				} else {
					errChan <- fmt.Errorf("unexpected return from Read: %d != %d or %s != nil", n, len(data), err)
				}
			}(reader, data, errChan)
		}

		requireWrite(t, data, writer)

		// Check we got the expected number of responses back after writing
		for range readers {
			require.NoError(t, <-errChan)
		}

	}

	// Have readers block
	errChan := make(chan error)
	for _, reader := range readers {
		go func(reader io.Reader, errChan chan error) {
			buf := make([]byte, 1000)
			n, err := reader.Read(buf)
			if n == 0 && err == io.EOF {
				errChan <- nil
			} else {
				errChan <- fmt.Errorf("unexpected return from Read: %d != 0 or %s != io.EOF", n, err)
			}
		}(reader, errChan)
	}

	require.NoError(t, writer.Close())

	// Check we got the expected number of responses back after closing
	for range readers {
		require.NoError(t, <-errChan)
	}

}

// TestFuzz launches a series of readers, doing random-sized reads, and a writer, doing random-sized writes.
// All readers should end up with the same data at the end.
func TestFuzz(t *testing.T) {
	writer, mb := multibuffer.New()

	data := randBytes(t, 10000+mathrand.Intn(100000))

	errChan := make(chan error)

	go func(writer io.WriteCloser, data []byte, errChan chan error) {
		for len(data) > 0 {
			amount := 1 + mathrand.Intn(len(data))
			n, err := writer.Write(data[:amount])
			data = data[amount:]
			if err != nil {
				panic(err)
			}
			if n != amount {
				errChan <- fmt.Errorf("wrote wrong amount %d != %d", n, amount)
				return
			}
		}
		errChan <- writer.Close()
	}(writer, data, errChan)

	readers := mathrand.Intn(100)
	for i := 0; i < readers; i++ {
		go func(reader io.ReadSeeker, data []byte, errChan chan error) {
			gotData := make([]byte, 0, len(data))
			for {
				amount := mathrand.Intn(len(data))
				buf := make([]byte, amount)
				n, err := reader.Read(buf)
				if err != nil && err != io.EOF {
					errChan <- err
					return
				}

				gotData = append(gotData, buf[:n]...)

				if err == io.EOF {
					if !bytes.Equal(gotData, data) {
						errChan <- fmt.Errorf("gotData wasn't expected: %v != %v", gotData, data)
					}
					errChan <- nil
					return
				}
			}

		}(mb.Reader(), data, errChan)
	}

	// One bonus reader:
	gotData, err := ioutil.ReadAll(mb.Reader())
	require.NoError(t, err)
	require.Equal(t, data, gotData)

	// readers + the writer
	for i := 0; i < readers+1; i++ {
		require.NoError(t, <-errChan)
	}
}

func TestReaderSeek(t *testing.T) {
	writer, mb := multibuffer.New()
	reader := mb.Reader()

	// No bytes written or read, so no matter whence, a 0 offset seek puts us at 0
	for _, whence := range []int{io.SeekStart, io.SeekCurrent, io.SeekEnd} {
		where, err := reader.Seek(0, whence)
		require.NoError(t, err)
		require.Zero(t, where)
	}

	data := randBytes(t, 1024)
	requireWrite(t, data, writer)

	// Seek from start to the end
	requireSeek(t, 1024, io.SeekStart, 1024, reader)

	// Seek to middle:
	requireSeek(t, -512, io.SeekCurrent, 512, reader)
	requireRead(t, data[512:], reader)
	requireSeek(t, -512, io.SeekEnd, 512, reader)
	requireRead(t, data[512:], reader)
	requireSeek(t, 512, io.SeekStart, 512, reader)
	requireRead(t, data[512:], reader)

	// Append more data and close
	requireWrite(t, data, writer)
	require.NoError(t, writer.Close())

	// seek to end
	end, err := reader.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	require.EqualValues(t, 2048, end)

	// At end, should read EOF
	buf := make([]byte, 1)
	n, err := reader.Read(buf)
	require.Zero(t, n)
	require.Equal(t, io.EOF, err)

	// Seek back to start and we should be able to read the whole thing
	start, err := reader.Seek(0, io.SeekStart)
	require.NoError(t, err)
	require.Zero(t, start)

	buf = make([]byte, 2048)
	n, err = reader.Read(buf)
	require.EqualValues(t, 2048, n)
	require.NoError(t, err)

	require.Equal(t, data, buf[1024:])
	require.Equal(t, data, buf[:1024])
}
