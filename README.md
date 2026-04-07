# pipeline

Simple Go package for implementing pipelines.  The pipeline stages
must be arranged as a finite directed acyclic graph with spouts as
entry points and sinks as exit points.  It is not checked whether the
pipeline stages are acyclic.

Spouts read input via io.Reader and sinks write output via io.Writer.
Pipeline stages, including spouts and sinks, run concurrently as
goroutines.  A stage receives data over an input channel, transforms
it and/or updates its state, and sends data over an output channel to
the next pipeline stage.

## Example

```Go
package main

import "bytes"
import "fmt"
import "github.com/flxch/pipeline"

func main() {
    inbuf := bytes.NewBuffer([]byte("Hello, World!"))
    outbuf := bytes.NewBuffer(nil)

    // Assemble pipeline.  No logging, channel size 1, do not pause when no
    // input at spout.  Since InputSize is 1, the spout reads byte by byte from
    // inbuf.
    p := pipeline,New(nil, 1, 1, 0)
    // Spout: Reads from inbuf and converts it to int.
    inch := pipeline.AddSpout(p, "spout", inbuf,
        func(in []byte) (int, error) { return int(in[0]), nil })
    // Stage: Filters data (only forward upper and lower case letters to the
    // next stage).  The stage's input channel is the output channel of the
    // spout.
    outch := pipeline.AddStage(p, "stage", inch,
        func(n int, out chan<- int) error {
            if n >= int('A') && n <= int('z') {
                out <- n
            }
            return nil
        })
    // Sink: Converts data to []byte and writes it to outbuf.  The sink's input
    // channel is the output channel of the filter stage.
    pipeline.AddSink(p, "sink", outch, outbuf,
        func(data int) ([]byte, error) { return []byte{byte(data)}, nil })

    // Run pipeline.
    p.Run()
    // Wait until the input buffer is empty.
    for inbuf.Len() > 0 { }
    // Close pipeline.
    if err := p.Close(); err != nil {
        panic("failed to close pipeline")
    }

    // Print received data.
    fmt.Printf("%s\n", outbuf.Bytes())
}
```

## Notes

The package is under development.  Its API might change.  Feedback is
welcomed.

## Alternative Pipeline Packages

Incomplete list of pipeline packages:

* https://github.com/deliveryhero/pipeline
* ...

