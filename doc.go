package pipeline

// A simple package to assemble a pipeline with different stages, where the
// pipeline stages run concurrently in goroutines.  Each stage has an input and
// an output channel, except the spouts and sinks.  Spouts read the input
// through an io.Reader and sinks write their output through an io.Writer.

// The pipeline implementation uses generics (supported by Go 1.18 or later) for
// the data sent through the pipeline.
// The functions like `AddSpout` and `AddStage` should actually be methods of
// the pipeline type.  However, Go currently does not support methods with type
// parameters.  Making the pipeline type generic does not help.  Hence, the
// package provides functions instead of methods for adding stages to the
// pipeline.

