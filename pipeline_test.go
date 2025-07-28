package pipeline

import (
    "bytes"
    "fmt"
    "testing"
    "log/slog"
    "sync"
    "time"
)


// `sliceBuffer` is used for testing to read values from a slice incrementally.
// To this end, it implements the `io.Reader` interface.  A call to the `Read`
// method reads the next data item from the buffer.
type sliceBuffer[Data any] struct {
    content []Data
    mux     sync.Mutex
}

func newSliceBuffer[Data any](ds []Data) *sliceBuffer[Data] {
    return &sliceBuffer[Data]{
        content: ds,
    }
}

// Make sliceBuffer an instance of the interface io.Reader.
func (b *sliceBuffer[Data]) Read(bs []Data) (int, error) {
    // Make reading from the buffer thread safe, i.e., multiple goroutines can
    // read from the buffer.
    b.mux.Lock()
    defer b.mux.Unlock()

    if len(b.content) == 0 {
        // Nothing to read.
        return 0, nil
    }
    if len(bs) == 0 {
        return 0, fmt.Errorf("empty slice")
    }
    // Copy data value from the buffer to the "output" slice.
    bs[0] = b.content[0]
    // Discard read value from buffer.
    b.content = b.content[1:]
    return 1, nil
}

// Return the length of the buffer, i.e., the number of items that can still be
// read.
func (b *sliceBuffer[Data]) Len() int {
    b.mux.Lock()
    defer b.mux.Unlock()

    return len(b.content)
}


// Basic test of the buffer implementation (not using a pipeline).
func TestSliceBuffer(t *testing.T) {
    input := "Hello, World!"
    buf := newSliceBuffer([]byte(input))
    output := make([]byte, len(input) + 1)

    if buf.Len() != len(input) {
        t.Errorf("invalid buffer len")
    }

    for i := 0; i < len(input); i++ {
        t.Logf("%d: input character %c", i, input[i])
        if n, err := buf.Read(output[i:]); err != nil {
            t.Fatalf("%d: failed reading from buffer: %v", i, err)
        } else if n != 1 {
            t.Fatalf("%d: failed to read value from buffer", i)
        } else if output[i] != byte(input[i]) {
            t.Errorf("%d: wrong value: expected %c, not %c", i, input[i], output[i])
        }
    }

    if n, err := buf.Read(output); err != nil {
        t.Fatalf("failed reading from buffer: %v", err)
    } else if n != 0 {
        t.Errorf("no value should have been read")
    }

    if buf.Len() > 0 {
        t.Errorf("nonempty buffer")
    }
}


func TestInOut(t *testing.T) {
    input := "Hello, World!"
    inbuf := newSliceBuffer([]byte(input))
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
    for {
        if inbuf.Len() == 0 {
            break
        }
    }

    // Close pipeline.  This includes waiting until all the goroutines of the
    // pipeline stages terminated.
    if err := p.Close(); err != nil {
        t.Fatalf("failed to close pipeline: %v", err)
    }

    // Check whether output matches the given input.
    if output := outbuf.String(); input != output {
        t.Errorf(`wrong value: expected "%s", not "%s"`, input, output)
    }
}

func TestStage(t *testing.T) {
    input := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
    inbuf := newSliceBuffer(input)
    outbuf := bytes.NewBuffer(nil)

    // Create a simple pipeline with a transformer stage that increments the
    // received values.
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
            if m := (n + 1) % 2; m != 0 {
                t.Logf("stage: dropping %d", n)
            } else {
                t.Logf("stage: transforming %d -> %d", n, m)
                out <- m
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
    for {
        if inbuf.Len() == 0 {
            break
        }
    }

    // Close pipeline.  This includes waiting until all the goroutines of the
    // pipeline stages terminated.
    if err := p.Close(); err != nil {
        t.Fatalf("failed to close pipeline: %v", err)
    }
}

func TestTransformer(t *testing.T) {
    input := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
    inbuf := newSliceBuffer(input)
    outbuf := bytes.NewBuffer(nil)

    // Create a simple pipeline with a transformer stage that increments the
    // received values.
    p := New(slog.Default(), 16, 1024, 0)
    inch := AddSpout(p, "input", inbuf,
        func(in []byte) (int, error) {
            t.Logf("input: %v", in)
            if len(in) != 1 {
                return 0, fmt.Errorf("invalid input value")
            }
            n := int(in[0])
            return n, nil
        })
    outch := AddTransformer(p, "transformer", inch,
        func(n int) (int, error) {
            m := n + 1
            t.Logf("transfom: %d -> %d", n, m)
            return m, nil
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
    for {
        if inbuf.Len() == 0 {
            break
        }
    }

    // Close pipeline.  This includes waiting until all the goroutines of the
    // pipeline stages terminated.
    if err := p.Close(); err != nil {
        t.Fatalf("failed to close pipeline: %v", err)
    }

    // Check whether output matches the given input.
    if output := outbuf.Bytes(); len(output) != len(input) {
        t.Errorf("number of received values does not match: expected %d values, not %d", len(input), len(output))
    } else {
        for i := 0; i < len(output); i++ {
            if output[i] != input[i] + 1 {
                t.Errorf("%d: wrong value: expected %d, not %d", i, input[i] + 1, output[i])
            }
        }
    }
}

func TestStageMulti(t *testing.T) {
    input := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
    inbuf := newSliceBuffer(input)
    outbufEven := bytes.NewBuffer(nil)
    outbufOdd := bytes.NewBuffer(nil)

    // Create a simple pipeline with a transformer stage that increments the
    // received values.
    p := New(slog.Default(), 16, 1024, 0)
    inch := AddSpout(p, "input", inbuf,
        func(in []byte) (int, error) {
            t.Logf("input: %v", in)
            if len(in) != 1 {
                return 0, fmt.Errorf("invalid input value")
            }
            n := int(in[0])
            return n, nil
        })
    // Stage with a single input and two next stages: one for even numbers and
    // one for odd numbers.  Nonpositive numbers are dropped.
    outchs := AddStageNM(p, "stage", []<-chan int{inch}, 2,
        func(n int, chs ...chan<- int) error {
            t.Logf("stage: %d", n)
            if n <= 0 {
                t.Logf("stage: dropping nonpositive value")
            } else if n % 2 == 0 {
                chs[0] <- n
            } else {
                chs[1] <- n
            }
            return nil
        })
    AddSink(p, "even", outchs[0], outbufEven,
        func(n int) ([]byte, error) {
            if n > 255 {
                return nil, fmt.Errorf("value too big")
            }
            t.Logf("output even: %d", n)
            return []byte{byte(n)}, nil
        })
    AddSink(p, "odd", outchs[1], outbufOdd,
        func(n int) ([]byte, error) {
            if n > 255 {
                return nil, fmt.Errorf("value too big")
            }
            t.Logf("output odd: %d", n)
            return []byte{byte(n)}, nil
        })

    // Run pipeline.
    p.Run()
    // Wait until the input buffer is empty.
    for {
        if inbuf.Len() == 0 {
            break
        }
    }

    // Close pipeline.  This includes waiting until all the goroutines of the
    // pipeline stages terminated.
    if err := p.Close(); err != nil {
        t.Fatalf("failed to close pipeline: %v", err)
    }
    for i, b := range outbufEven.Bytes() {
        if b % 2 != 0 {
            t.Errorf("even %d: expected an even number", i)
        }
    }
    for i, b := range outbufOdd.Bytes() {
        if b % 2 != 1 {
            t.Errorf("even %d: expected an even number", i)
        }
    }
}

