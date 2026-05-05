package lowstate

import "github.com/ruffel/brine"

// Entry is a raw Salt lowstate entry.
type Entry = brine.LowstateEntry

// Request constructs a raw lowstate request.
func Request(entries ...Entry) brine.Request {
	return brine.Request{
		Kind:     brine.KindLowstate,
		Lowstate: append([]brine.LowstateEntry(nil), entries...),
	}
}
