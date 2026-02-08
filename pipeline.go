package pipeline

import (
    "fmt"
    "io"
    "log/slog"
    "sync"
    "time"
)


// Pipeline struct, including default parameters for stages.
type P struct {
    // Options (default values for pipeline stages)
    // Log pipeline errors and events.  No logging if nil.
    Logger    *slog.Logger
    // Channel size between stages.
    ChanSize  int
    // Size of byte slice for reading input at spout (only for spouts).  Size
    // must be large enough so that any received input can be stored in the slice.
    InputSize int
    // Pause reading input when no input at spout (only for spouts).
    Pause     time.Duration

    // Internals
    // Channel to signal the spouts to start reading input.
    start     chan struct{}
    // Channel to signal the spouts to terminate.
    done      chan struct{}
    // The WaitGroup is used for waiting until all pipeline stages terminated
    // when closing the pipeline.
    wg        *sync.WaitGroup
}


// `New` creates a new pipeline.  The function's arguments `l` (logger for the
// stages; no logging if nil), `chsz` (output channel size of a stage), `insz`
// (input size of a spout), and `dur` (pause between inputs) are default
// values for the pipeline's option parameters.  They can be overwritten
// individually for each stage.
// Spouts, sinks, and stages are added by the functions `AddSpout`, `AddSink`,
// and `AddStage`, respectively.  After setting up the stages, the method `Run`
// activates the pipeline.
func New(l *slog.Logger, chsz, insz int, dur time.Duration) P {
    return P{
        Logger:    l,
        ChanSize:  chsz,
        InputSize: insz,
        Pause:     dur,
        start:     make(chan struct{}),
        done:      make(chan struct{}),
        wg:        &sync.WaitGroup{},
    }
}


// `isRunning` returns true if the pipeline `p` is running, i.e., the method
// `Run` has been called before.
func (p P) isRunning() bool {
    select {
    case <-p.start:
        // Since no data items are sent over the start channel, this case is
        // blocked when the start channel has not been closed.  The start
        // channel is closed by the Run method, which "starts" the pipeline's
        // spouts to receive input.
        return true
    default:
        // If the start channel is not closed, the default case is executed,
        // since the above case is blocked.
        return false
    }
}

// `isClosed` returns true if the pipeline `p` has been closed, i.e., the method
// `Close` has been called before.
func (p P) isClosed() bool {
    select {
    case <-p.done:
        return true
    default:
        return false
    }
}


// `Close` closes the pipeline `p`.  This includes signaling to all spouts of
// the pipeline to terminate.  The terminate signal is forward to the next
// pipeline stage, e.g., a spout signals its next pipeline stage to terminate.
// `Close` waits until all stages of the pipeline have terminated.
func (p P) Close() error {
    if !p.isRunning() {
        return fmt.Errorf("pipeline is not running")
    }
    if p.isClosed() {
        return fmt.Errorf("pipeline is already closed")
    }

    // Signal the goroutines for the spouts to terminate.
    close(p.done)
    // Wait until all pipeline stages have been terminated.
    p.wg.Wait()

    return nil
}

// `Run` sends signals to the pipeline spouts that the pipeline `p` is ready and
// that they are now ready to receive input data.
func (p P) Run() {
    if p.isRunning() {
        panic("pipeline is already running")
    }
    if p.isClosed() {
        panic("pipeline is closed")
    }

    // Signal the goroutines of the spouts to receive inputs.
    close(p.start)
}


// `AddSpout` adds a spout to the pipeline `p` with the identifier `id`.  The
// spout uses the reader `r` to continuously receive data items (as byte slices)
// and uses the function `op` to process the received items.  A typical use of
// `op` is to convert input to the corresponding data items of the pipeline's
// internal data type.  The function `AddSpout` returns an output channel from
// which the next pipeline stage receives its input.  Note that the input is
// dropped at the spout if the reader `r` reads zero bytes or if an error occurs
// while processing the received input (i.e., `op` returns an error).
func AddSpout[Data any](p P, id string, r io.Reader, op func([]byte) (Data, error)) <-chan Data {
    if p.isRunning() {
        panic("pipeline is running")
    }
    if p.isClosed() {
        panic("pipeline is closed")
    }

    // The output channel is the input channel of the next pipeline stage.
    out := make(chan Data, p.ChanSize)

    // Start a goroutine that continuously reads input (using the reader `r`)
    // and sends the processed data over the `out` channel to the next pipeline
    // stage.
    p.wg.Add(1)
    go func() {
        defer p.wg.Done()

        // Each input is a byte sequence that is stored in the slice `bs`.
        bs := make([]byte, p.InputSize)

        // Wait for the signal to start reading input data.
        <-p.start
        if p.Logger != nil {
            p.Logger.Info("Spout ready to receive input.", slog.String("id", id))
        }

        // Continuously read and process input data.
        for {
            select {
            case <-p.done:
                if p.Logger != nil {
                    p.Logger.Info("Spout shutting down.", slog.String("id", id))
                }
                // Signal the next pipeline stage to also shutdown.
                close(out)
                // Terminate.
                return
            default:
                // Read input data, process data, and if no error, send
                // processed data to the next pipeline stage.
                if n, err := r.Read(bs); err != nil {
                    if p.Logger != nil {
                        p.Logger.Error("Failed to read input at spout.", slog.String("id", id), slog.Any("error", err))
                    }
                } else if n == 0 {
                    if p.Logger != nil {
                        p.Logger.Info("Empty input at spout.", slog.String("id", id))
                    }
                    if p.Pause > 0 {
                        // If no input is read, pause some time before trying to
                        // read the next input.  The delay prevents creating a
                        // hot loop.  Alternatively, we could call
                        // runtime.Gosched here.  This would allow the Go's
                        // scheduler to run other goroutines.
                        time.Sleep(p.Pause)
                    }
                } else if n > len(bs) {
                    // If input is too big, drop input.
                    if p.Logger != nil {
                        p.Logger.Error("Failed to read input.", slog.String("id", id), slog.Any("error", fmt.Errorf("input with more than %d bytes", n)), slog.Any("data", bs))
                    }
                } else if data, err := op(bs[:n]); err != nil {
                    // If operation fails on input, drop input.
                    if p.Logger != nil {
                        p.Logger.Error("Failed operation at spout.", slog.String("id", id), slog.Any("error", err))
                    }
                } else {
                    // Send data.
                    out <- data
                }
            }
        }
    }()

    // Return the output channel of the spout, which is the input channel of the
    // next stage.
    return out
}

// `AddSink` adds a sink with the identifier `id` to the pipeline `p`.  The sink
// receives its input over the channel `in`.  For each received value, the sink
// performs `op` on it before writing the output via the writer `w`.  The value
// is dropped at the sink if the operation `op` returns the nil slice or `op`
// returns an error.
func AddSink[Data any](p P, id string, in <-chan Data, w io.Writer, op func(Data) ([]byte, error)) {
    if p.isRunning() {
        panic("pipeline is running")
    }
    if p.isClosed() {
        panic("pipeline is closed")
    }

    // Start a goroutine that contiunously receives the input over the `in`
    // channel and outputs the processed data via the writer `w`.
    p.wg.Add(1)
    go func() {
        defer p.wg.Done()

        for data := range in {
            if bs, err := op(data); err != nil {
                if p.Logger != nil {
                    p.Logger.Error("Failed operation at sink.", slog.String("id", id), slog.Any("error", err))
                }
            } else if bs == nil {
                if p.Logger != nil {
                    p.Logger.Info("Empty output at sink.", slog.String("id", id), slog.Any("data", data))
                }
            } else if _, err := w.Write(bs); err != nil && p.Logger != nil {
                p.Logger.Error("Failed to write output at sink.", slog.String("id", id), slog.Any("error", err))
            }
        }

        // Report termination of the sink.
        if p.Logger != nil {
            p.Logger.Info("Sink shutting down.", slog.String("id", id))
        }
    }()
}


// `AddStage` adds a pipeline stage with identifier `id` to the pipeline `p`.
// The stage receives input over `in` channel.  Each input item is processed by
// the function `op`.  The function returns a channel that is the input of the
// next pipeline stage.  The `op` function is called with the output channel.
func AddStage[DataIn, DataOut any](p P, id string, in <-chan DataIn, op func(DataIn, chan<- DataOut) error) <-chan DataOut {
    if p.isRunning() {
        panic("pipeline is running")
    }
    if p.isClosed() {
        panic("pipeline is closed")
    }

    // The output channel is the input channel of the next pipeline stage.
    out := make(chan DataOut, p.ChanSize)

    // Start a goroutine that continuously receives data items over the provided
    // input channel `in`.  Furthermore, the stage operation `op` (while
    // processing an input data item) can send data items over the output channel
    // to the next pipeline stage.
    p.wg.Add(1)
    go func() {
        defer p.wg.Done()

        for data := range in {
            if err := op(data, out); err != nil && p.Logger != nil {
                p.Logger.Error("Failed operation at stage.", slog.String("id", id), slog.Any("error", err))
            }
        }

        // Report termination of the stage.
        if p.Logger != nil {
            p.Logger.Info("Stage shutting down.", slog.String("id", id))
        }
        // Signal the next pipeline stage to also terminate.
        close(out)
    }()

    return out
}
