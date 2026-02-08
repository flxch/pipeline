package pipeline

// A simple package to assemble a pipeline with different stages, where the
// pipeline stages run concurrently as goroutines.  Each stage has an input and
// an output channel, except the spouts and sinks.  Spouts read the input
// through an io.Reader and sinks write their output through an io.Writer.  The
// output channel of a stage must be used as an input channel of at least one
// stage.  It can be the input channel of multiple stages though.

// A stage can be stateless (transformer) or stateful.  The package also
// supports stages with multiple input and output channels.

// The pipeline implementation uses generics (supported by Go 1.18 or later) for
// the data sent through the pipeline.  The functions like `AddSpout` and
// `AddStage` should actually be methods of the pipeline type `P`.  However, Go
// currently does not support methods with type parameters.
