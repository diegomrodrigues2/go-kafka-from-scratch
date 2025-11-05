package broker

type Config struct {
	DataDir      string
	SegmentBytes int64
}

func DefaultConfig() Config {
	return Config{
		DataDir:      "./data",
		SegmentBytes: 128 << 20,
	}
}
