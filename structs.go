package brine

type TargetType string

const (
	TargetGlob     TargetType = "glob"
	TargetList     TargetType = "list"
	TargetCompound TargetType = "compound"
)

type Target struct {
	Value string
	Type  TargetType
}

type Request struct {
	Target   Target
	Function string
	Args     []string
	Kwargs   map[string]string
}

type Response struct {
	Data []byte
}
