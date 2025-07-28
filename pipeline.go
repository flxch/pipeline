package pipeline

import (
    "fmt"
    "io"
    "log/slog"
    "sync"
    "time"
)


type Pipeline struct {
    // Options (default values for pipeline stages)
    Logger    *slog.Logger
    ChanSize  int
    InputSize int           // only applies to spouts
    Pause     time.Duration // only applies to spouts

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
// stages; no logging if nil), `chsz` (output channel size of a stage), `isz`
// (input size of a spout), and `pause` (pause between inputs) are default
// values for the pipeline's option parameters.  They can be overwritten
// individually for each stage.
// Spouts, sinks, and stages are added by the functions `AddSpout`, `AddSink`,
// and `AddStage`, respectively.  After setting up the stages, the method `Run`
// activates the pipeline.
func New(l *slog.Logger, chsz, isz int, dur time.Duration) Pipeline {
    return Pipeline{
        Logger:    l,
        ChanSize:  chsz,
        InputSize: isz,
        Pause:     dur,
        start:     make(chan struct{}),
        done:      make(chan struct{}),
        wg:        &sync.WaitGroup{},
    }
}


// `isRunning` returns true if the pipeline `p` is running, i.e., the method
// `Run` has been called before.
func (p Pipeline) isRunning() bool {
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
func (p Pipeline) isClosed() bool {
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
func (p Pipeline) Close() error {
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
func (p Pipeline) Run() {
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
// and uses the function `op` to process the received items.  The function
// returns an output channel from which the next pipeline stage receives its
// input.  Note that input is dropped at the spout if the reader `r` reads zero
// bytes or if an error occurs while processing the received input (i.e., `op`
// returns an error).
func AddSpout[Data any](p Pipeline, id string, r io.Reader, op func([]byte) (Data, error)) <-chan Data {
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
                        // scheduler to run other goroutine.
                        time.Sleep(p.Pause)
                    }
                } else if data, err := op(bs[:n]); err != nil {
                    if p.Logger != nil {
                        p.Logger.Error("Failed operation at spout.", slog.String("id", id), slog.Any("error", err))
                    }
                } else {
                    out <- data
                }
            }
        }
    }()

    // Return the output channel of the spout, which is the input channel of the
    // next stage.
    return out
}

// The value is dropped at the sink if the operation `op` returns the nil slice.
func AddSink[Data any](p Pipeline, id string, in <-chan Data, w io.Writer, op func(Data) ([]byte, error)) {
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
func AddStage[DataIn, DataOut any](p Pipeline, id string, in <-chan DataIn, op func(DataIn, chan<- DataOut) error) <-chan DataOut {
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


// `AddTransformer` is a special case of `AddStage`, where each input item is
// transformed via the function `op` and sent to the next pipeline stage.
// `AddTransformer` returns the output channel, which is the input channel of
// the next stage.
// `AddTransformer` might get deprecated, since a transformer stage can be
// easily implemented by `AddStage` function without disadvantages.
func AddTransformer[DataIn, DataOut any] (p Pipeline, id string, in <-chan DataIn, op func(DataIn) (DataOut, error)) <-chan DataOut {
    if p.isRunning() {
        panic("pipeline is running")
    }
    if p.isClosed() {
        panic("pipeline is closed")
    }

    // The output channel is the input channel of the next pipeline stage.
    out := make(chan DataOut, p.ChanSize)

    // Start a goroutine that continuously receives data items over the input
    // channel and sends the processed items over the output channel to the next
    // pipeline stage.
    p.wg.Add(1)
    go func() {
        defer p.wg.Done()

        for datain := range in {
            if dataout, err := op(datain); err != nil {
                if p.Logger != nil {
                    p.Logger.Error("Failed operation at transformer.", slog.String("id", id), slog.Any("error", err))
                }
            } else {
                out <- dataout
            }
        }

        // Report termination of the stage.
        if p.Logger != nil {
            p.Logger.Info("Transformer shutting down.", slog.String("id", id))
        }
        // Signal the next pipeline stages to also terminate.
        close(out)
    }()

    return out
}


// Note that the function `op` must thread-safe as multiple goroutines may
// execute it at the same time.  We could functions like AddStage2M that
// receives input from 2 input channels, where the `op` function does not need
// to be thread-safe.  Unfortunately, Go does not allow us to implement such a
// function directly, where N is not fixed.  (It seems that such an implemention
// would be possible by using the refelect package and its function Select().
// However, such an implementation would most likely have poor performance.)
// Overall, the pipeline stage AddStageNM runs not a single goroutine.  Instead,
// the stage consists of N+1 goroutines.
func AddStageNM[DataIn, DataOut any](p Pipeline, id string, ins []<-chan DataIn, degree int, op func(DataIn, ...chan<- DataOut) error) []<-chan DataOut {
    if p.isRunning() {
        panic("pipeline is running")
    }
    if p.isClosed() {
        panic("pipeline is closed")
    }

    // The output channels of this stage are the input channels of the next
    // pipeline stages.  Because of typing constraints we must convert the
    // channels into slices for input and output channels.
    chs := make([]chan DataOut, degree)
    outs := make([]chan<- DataOut, degree)
    rets := make([]<-chan DataOut, degree)
    for i := 0; i < degree; i++ {
        chs[i] = make(chan DataOut, p.ChanSize)
        outs[i] = chs[i]
        rets[i] = chs[i]
    }

    // For each input channel of this stage, start a goroutine that continuously
    // receives data items.  The operation can send data items over the output
    // channels to the next pipeline stage while processing an input data item.
    // Note that there is no guarantee on the order how the data items are
    // processed from different input channels.
    var wg sync.WaitGroup
    wg.Add(len(ins))
    for i := 0; i < len(ins); i++ {
        go func(i int) {
            defer wg.Done()

            for data := range ins[i] {
                if err := op(data, outs...); err != nil && p.Logger != nil {
                    p.Logger.Error("Failed operation at stage.", slog.String("id", id), slog.Any("error", err))
                }
            }
        }(i)
    }

    // Start a goroutine that waits until all input channels of this stage have
    // been closed.
    p.wg.Add(1)
    go func() {
        defer p.wg.Done()

        // Wait until all goroutines of this stage have terminated.
        wg.Wait()
        if p.Logger != nil {
            p.Logger.Info("Stage shutting down.", slog.String("id", id))
        }
        // Signal the next pipeline stages to also terminate.
        for i := 0; i < degree; i++ {
            close(outs[i])
        }
    }()

    return rets
}

func AddTransformerNM[DataIn, DataOut any] (p Pipeline, id string, ins []<-chan DataIn, degree int, op func(DataIn) (DataOut, int, error)) []<-chan DataOut {
    if p.isRunning() {
        panic("pipeline is running")
    }
    if p.isClosed() {
        panic("pipeline is closed")
    }

    // The output channels of this stage are the input channels of the next
    // pipeline stages.
    outs := make([]chan DataOut, degree)
    for i := 0; i < degree; i++ {
        outs[i] = make(chan DataOut, p.ChanSize)
    }

    // For each input channel of this stage, start a goroutine that continuously
    // receives data items and sends the processed items over the respective
    // output channel to the next pipeline stage.  Note that there is no
    // guarantee on the order how the data items are processed from different
    // input channels.
    var wg sync.WaitGroup
    wg.Add(len(ins))
    for i := 0; i < len(ins); i++ {
        go func(i int) {
            defer wg.Done()

            for datain := range ins[i] {
                if dataout, n, err := op(datain); err != nil {
                    if p.Logger != nil {
                        p.Logger.Error("Failed operation at transformer.", slog.String("id", id), slog.Any("error", err))
                    }
                } else if n < 0 || n > degree {
                    if p.Logger != nil {
                        p.Logger.Info("Dropping input at transformer.", slog.String("id", id), slog.Any("data", datain))
                    }
                } else {
                    outs[n] <- dataout
                }
            }
        }(i)
    }

    // Start a goroutine that waits until all input channels of this stage have
    // been closed.
    p.wg.Add(1)
    go func() {
        defer p.wg.Done()

        // Wait until all goroutines of this stage have terminated.
        wg.Wait()
        if p.Logger != nil {
            p.Logger.Info("Transformer shutting down.", slog.String("id", id))
        }
        // Signal the next pipeline stages to shutdown before exiting this stage.
        for i := 0; i < degree; i++ {
            close(outs[i])
        }
    }()

    rets := make([]<-chan DataOut, degree)
    for i := 0; i < degree; i++ {
        rets[i] = outs[i]
    }
    return rets
}
