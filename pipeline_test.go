package pipeline

import (
    "bytes"
    "fmt"
    "testing"
    "log/slog"
    "slices"
    "sync"
    "time"
)


// Examples.

// `ExampleP` setups a pipeline with a filtering stage: spout -> filter -> sink.
func ExampleP() {
    inbuf := bytes.NewBuffer([]byte("Hello, World!"))
    outbuf := bytes.NewBuffer(nil)

    // Assemble pipeline.  No logging, channel size 1, do not pause when no
    // input at spout.  Since InputSize is 1, the spout reads byte by byte from
    // inbuf.
    p := New(nil, 1, 1, 0)
    // Spout: Reads from inbuf and converts it to int.
    inch := AddSpout(p, "spout", inbuf, func(in []byte) (int, error) { return int(in[0]), nil })
    // Stage: Filters data (only forward upper and lower case letters to the
    // next stage).  The stage's input channel is the output channel of the
    // spout.
    outch := AddStage(p, "stage", inch,
        func(n int, out chan<- int) error {
            if n >= int('A') && n <= int('z') {
                out <- n
            }
            return nil
        })
    // Sink: Converts data to []byte and writes it to outbuf.  The sink's input
    // channel is the output channel of the filter stage.
    AddSink(p, "sink", outch, outbuf, func(data int) ([]byte, error) { return[]byte{byte(data)}, nil })

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

    // Output:
    // HelloWorld
}


// Tests.

// The `input` type is used for testing.  The input data is stored in a slice.
// `input` implements the `io.Reader` interface.  A call to the `Read` method
// reads the next data item from the slice.
type input struct {
    content []byte
    mux     sync.Mutex
}

// Create test input data.
func newInput(ds []byte) *input {
    return &input{
        content: ds,
    }
}

// Make the input type an instance of the interface io.Reader.
func (b *input) Read(bs []byte) (int, error) {
    // Make reading from the buffer thread safe, i.e., multiple goroutines can
    // read from the buffer.
    b.mux.Lock()
    defer b.mux.Unlock()

    if len(b.content) == 0 {
        // No input data (anymore).
        return 0, nil
    }
    if len(bs) == 0 {
        // No space to store input data.
        return 0, fmt.Errorf("empty slice")
    }

    // Copy data value from the buffer to the "output" slice.
    bs[0] = b.content[0]
    // Discard read value from buffer.
    b.content = b.content[1:]
    return 1, nil
}

// Return the number of data items that can still be read.
func (b *input) Len() int {
    b.mux.Lock()
    defer b.mux.Unlock()

    return len(b.content)
}

// Basic tests of the input implementation (not using a pipeline).
func TestInput(t *testing.T) {
    in := "Hello, World!"
    buf := newInput([]byte(in))
    out := make([]byte, len(in) + 1)

    if buf.Len() != len(in) {
        t.Errorf("invalid buffer len")
    }

    for i := 0; i < len(in); i++ {
        t.Logf("%d: input character '%c'", i, in[i])
        if n, err := buf.Read(out[i:]); err != nil {
            t.Fatalf("%d: failed reading from buffer: %v", i, err)
        } else if n != 1 {
            t.Fatalf("%d: failed to read value from buffer", i)
        } else if out[i] != byte(in[i]) {
            t.Errorf("%d: wrong value: expected '%c', not '%c'", i, in[i], out[i])
        }
    }

    if n, err := buf.Read(out); err != nil {
        t.Fatalf("failed reading from buffer: %v", err)
    } else if n != 0 {
        t.Errorf("no value should have been read")
    }

    if buf.Len() > 0 {
        t.Errorf("nonempty buffer")
    }
}


// Test for a pipeline that transfers the input from the spout to the sink.
func TestInOut(t *testing.T) {
    input := "Hello, World!"
    inbuf := newInput([]byte(input))
    outbuf := bytes.NewBuffer(nil)

    // Create a simple pipeline with a single spout and a single sink.  Read
    // values are copied to the output.
    p := New(slog.Default(), 16, 1024, 100 * time.Microsecond)
    ch := AddSpout(p, "input", inbuf,
        func(in []byte) (byte, error) {
            if len(in) != 1 {
                return 0, fmt.Errorf("failed to convert input data")
            }
            t.Logf("input: %c", in[0])
            return in[0], nil
        })
    AddSink(p, "output", ch, outbuf,
        func(data byte) ([]byte, error) {
            t.Logf("output: %c", data)
            return[]byte{data}, nil
        })
    // Remark: It would be more memory efficient when creating a byte slice (of
    // length 1) globally once, outside of the sink's function and just write
    // the data item at position 0 to it.  However, if the slice is processed
    // concurrently by the sink's writer, we run into concurrency issues as the
    // data item can be overwritten.  It is "saver" to create a byte slice (of
    // length 1) locally for each received data item.

    // Run pipeline.
    p.Run()
    // Wait until the input buffer is empty.
    for inbuf.Len() > 0 { }

    // Close pipeline.  This includes waiting until all the goroutines of the
    // pipeline stages terminated.
    if err := p.Close(); err != nil {
        t.Fatalf("failed to close pipeline: %v", err)
    }

    // Check whether output matches the input.
    if output := outbuf.String(); input != output {
        t.Errorf(`wrong value: expected "%s", not "%s"`, input, output)
    }
}

// Test for a pipeline with a stateless stage.
func TestStatelessStage(t *testing.T) {
    input := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 255}
    inbuf := newInput(input)
    outbuf := bytes.NewBuffer(nil)

    // Create a simple pipeline with a transformer stage that drops even numbers
    // and transforms odd numbers by incrementing them.  Values are also dropped
    // at the sink when they are too big.  Note that the spout and sink read and
    // output byte values whereas the stage operates in the int type.
    p := New(slog.Default(), 16, 1024, 100 * time.Microsecond)
    inch := AddSpout(p, "input", inbuf,
        func(in []byte) (int, error) {
            t.Logf("input: %v", in)
            if len(in) != 1 {
                return 0, fmt.Errorf("invalid input value")
            }
            n := int(in[0])
            return n, nil
        })
    outch := AddStage(p, "stage", inch,
        func(n int, out chan<- int) error {
            if n % 2 == 0 {
                t.Logf("stage: dropping %d", n)
            } else {
                t.Logf("stage: transforming %d -> %d", n, n + 1)
                out <- n + 1
            }
            return nil
        })
    AddSink(p, "output", outch, outbuf,
        func(n int) ([]byte, error) {
            if n > 255 {
                return nil, fmt.Errorf("value too big")
            }
            t.Logf("output: %d", n)
            return []byte{byte(n)}, nil
        })

    // Run pipeline.
    p.Run()
    // Wait until the input buffer is empty.
    for inbuf.Len() > 0 { }

    // Close pipeline.  This includes waiting until all the goroutines of the
    // pipeline stages terminated.
    if err := p.Close(); err != nil {
        t.Fatalf("failed to close pipeline: %v", err)
    }

    // Check output received by the sink.
    if expected, received := []byte{2, 4, 6, 8, 10}, outbuf.Bytes(); bytes.Compare(expected, received) != 0 {
        t.Errorf("wrong elements at sink: expected %v, not %v", expected, received)
    }
}

// Test for a pipeline with a stateful stage.
func TestStatefulStage(t *testing.T) {
    input := []byte{0, 1, 2, 3, 4, 0, 5, 1, 6, 7, 8, 0, 9, 255, 0, 1, 0, 2}
    inbuf := newInput(input)
    outbuf := bytes.NewBuffer(nil)

    p := New(slog.Default(), 16, 1024, 100 * time.Microsecond)
    inch := AddSpout(p, "input", inbuf,
        func(in []byte) (int, error) {
            t.Logf("input: %v", in)
            if len(in) != 1 {
                return 0, fmt.Errorf("invalid input value")
            }
            n := int(in[0])
            return n, nil
        })
    // The stage aggregates the received values.  It sends the aggregation if it
    // execeeds a threshold and resets the aggregation.
    var aggreg int
    outch := AddStage(p, "stage", inch,
        func(n int, out chan<- int) error {
            t.Logf("stage: state update %d -> %d", aggreg, aggreg + n)
            aggreg += n
            if aggreg > 9 {
                out <- aggreg
                aggreg = 0
            }
            return nil
        })
    AddSink(p, "output", outch, outbuf,
        func(n int) ([]byte, error) {
            if n > 255 {
                return nil, fmt.Errorf("value too big")
            }
            t.Logf("output: %d", n)
            return []byte{byte(n)}, nil
        })

    // Run pipeline.
    p.Run()
    // Wait until the input buffer is empty.
    for inbuf.Len() > 0 { }

    // Close pipeline.  This includes waiting until all the goroutines of the
    // pipeline stages terminated.
    if err := p.Close(); err != nil {
        t.Fatalf("failed to close pipeline: %v", err)
    }

    // Check output received by the sink.
    if expected, received := []byte{10, 12, 15}, outbuf.Bytes(); bytes.Compare(expected, received) != 0 {
        t.Errorf("wrong elements at sink: expected %v, not %v", expected, received)
    }
    // Check the state of the stage.
    if expected, state := 3, aggreg; expected != state {
        t.Errorf("wrong stage state: expected %v, not %v", expected, state)
    }
}


// Test for a pipeline with a stage that receives input from multiple spouts and
// outputs to multiple sinks.
func TestMultiStage(t *testing.T) {
    input := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
    inbuf := newInput(input)
    evens := bytes.NewBuffer(nil)
    odds := bytes.NewBuffer(nil)

    p := New(slog.Default(), 16, 1024, 100 * time.Microsecond)
    inop := func(in []byte) (int, error) {
        if len(in) != 1 {
            return 0, fmt.Errorf("invalid input value")
        }
        n := int(in[0])
        return n, nil
    }
    inch0 := AddSpout(p, "input0", inbuf, inop)
    inch1 := AddSpout(p, "input1", inbuf, inop)
    // The pipeline stage receives input over two channels.  It sends even
    // numbers to the first output channel and odd numbers to the second output
    // channel.
    outchs := AddStageNM(p, "stage", []<-chan int{inch0, inch1}, 2,
        func(n int, outs ...chan<- int) error {
            if n % 2 == 0 {
                outs[0] <- n
            } else {
                outs[1] <- n
            }
            return nil
        })
    outop := func(n int) ([]byte, error) {
        if n > 255 {
            return nil, fmt.Errorf("value too big")
        }
        return []byte{byte(n)}, nil
    }
    AddSink(p, "output0", outchs[0], evens, outop)
    AddSink(p, "output1", outchs[1], odds, outop)

    // Run pipeline.
    p.Run()
    // Wait until the input buffers are both empty.
    for inbuf.Len() > 0 { }

    // Close pipeline.  This includes waiting until all the goroutines of the
    // pipeline stages terminated.
    if err := p.Close(); err != nil {
        t.Fatalf("failed to close pipeline: %v", err)
    }

    // Check output received by the sinks.
    receivedEvens := evens.Bytes()
    slices.SortFunc(receivedEvens, func(a, b byte) int { return int(a) - int(b) })
    if expected := []byte{0, 2, 4, 6, 8}; bytes.Compare(expected, receivedEvens) != 0 {
        t.Errorf("wrong elements at even sink: expected %v, not %v", expected, receivedEvens)
    }
    receivedOdds := odds.Bytes()
    slices.SortFunc(receivedOdds, func(a, b byte) int { return int(a) - int(b) })
    if expected := []byte{1, 3, 5, 7, 9}; bytes.Compare(expected, receivedOdds) != 0 {
        t.Errorf("wrong elements at odd sink: expected %v, not %v", expected, receivedOdds)
    }
}

