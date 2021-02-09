# multireader

This package implements multireader.Buffer.

This buffer implements io.Writer to add new data, and the Reader method on it returns a new io.Reader which reads back
data until the Buffer is closed. This is useful for taking data from anything that can write to an io.Writer, and
multiplexing it out to multiple io.Readers.

The original use-case was in a process monitor that supported streaming the output of a job to web clients over HTTP.
