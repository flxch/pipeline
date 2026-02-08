package pipeline

import (
    "log/slog"
    "sync"
)


// Spout with multiple output channels.
// func AddSpoutM[DataOut any](p P, id string, r io.Reader, degree int, op func(DataIn, ...chan<- DataOut) error) []<-chan DataOut


// Sink with multiple input channels.
// func AddSinkN[DataIn any](p P, id string, ins []<-chan DataIn, w io.Writer, op func(DataIn) ([]byte, error))


// The function `AddStageNM` extends the function `AddStage`.  `AddStageNM` adds
// a stage with the identifier `id` to the pipeline `p` with multiple input
// channels and multiple output channels.  For each received data item, the
// stage performs the operation `op`, which also determines over which output
// channel the data is sent to the next pipeline stage.

// Note that the function `op` must be thread-safe as multiple goroutines may
// execute it at the same time.  This is important if the stage is stateful.  We
// could implement functions like AddStage2M that receive input from two input
// channels, where the `op` function does not need to be thread-safe.
// Unfortunately, Go does not allow us to implement such a function directly,
// where N is not fixed.  (It seems that such an implemention would be possible
// by using the refelect package and its function Select().  However, such an
// implementation would most likely have poor performance.)  Overall, the
// pipeline stage AddStageNM does not runs a single goroutine.  Instead, the
// stage consists of N+1 goroutines.
func AddStageNM[DataIn, DataOut any](p P, id string, ins []<-chan DataIn, degree int, op func(DataIn, ...chan<- DataOut) error) []<-chan DataOut {
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
    // been closed.  It then signals the next stages to also terminate.
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


