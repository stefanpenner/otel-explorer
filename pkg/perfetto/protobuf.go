package perfetto

// Minimal Perfetto protobuf encoder using raw wire format.
// This avoids importing Perfetto's full proto definitions while producing
// valid .pftrace files that Perfetto UI can open natively.
//
// Field numbers come from:
//   - protos/perfetto/trace/trace.proto
//   - protos/perfetto/trace/trace_packet.proto
//   - protos/perfetto/trace/track_event/track_event.proto
//   - protos/perfetto/trace/track_event/track_descriptor.proto
//   - protos/perfetto/trace/track_event/debug_annotation.proto

import (
	"encoding/binary"
	"math"
)

// Wire types
const (
	wireVarint  = 0
	wireFixed64 = 1
	wireBytes   = 2
)

// TrackEvent.Type enum values
const (
	typeSliceBegin = 1
	typeSliceEnd   = 2
	typeInstant    = 3
)

// protoWriter builds protobuf messages using raw wire encoding.
type protoWriter struct {
	buf []byte
}

func (w *protoWriter) bytes() []byte {
	return w.buf
}

// appendVarint writes a varint to the buffer.
func (w *protoWriter) appendVarint(v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	w.buf = append(w.buf, tmp[:n]...)
}

// appendTag writes a field tag (field_number << 3 | wire_type).
func (w *protoWriter) appendTag(field uint32, wireType uint32) {
	w.appendVarint(uint64(field)<<3 | uint64(wireType))
}

// writeVarintField writes a varint field, omitting it when v == 0
// (default proto semantics). Use writeVarintFieldAlways for fields
// where zero is meaningful, such as timestamps.
func (w *protoWriter) writeVarintField(field uint32, v uint64) {
	if v == 0 {
		return
	}
	w.appendTag(field, wireVarint)
	w.appendVarint(v)
}

// writeVarintFieldAlways writes a varint field, including v == 0,
// for fields where presence is explicit (e.g. timestamp).
func (w *protoWriter) writeVarintFieldAlways(field uint32, v uint64) {
	w.appendTag(field, wireVarint)
	w.appendVarint(v)
}

// writeInt64Field writes an int64 field as a plain two's-complement varint
// (proto int64 encoding, not zigzag/sint64). It always writes the field,
// including v == 0, for oneof members where presence is explicit.
func (w *protoWriter) writeInt64Field(field uint32, v int64) {
	w.appendTag(field, wireVarint)
	w.appendVarint(uint64(v))
}

// writeStringField writes a length-delimited string field.
func (w *protoWriter) writeStringField(field uint32, s string) {
	if s == "" {
		return
	}
	w.appendTag(field, wireBytes)
	w.appendVarint(uint64(len(s)))
	w.buf = append(w.buf, s...)
}

// writeBytesField writes a length-delimited bytes field.
func (w *protoWriter) writeBytesField(field uint32, b []byte) {
	if len(b) == 0 {
		return
	}
	w.appendTag(field, wireBytes)
	w.appendVarint(uint64(len(b)))
	w.buf = append(w.buf, b...)
}

// writeFixed64Field writes a fixed64 field.
func (w *protoWriter) writeFixed64Field(field uint32, v uint64) {
	if v == 0 {
		return
	}
	w.appendTag(field, wireFixed64)
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	w.buf = append(w.buf, tmp[:]...)
}

// writeDoubleField writes a double (float64) field as fixed64.
func (w *protoWriter) writeDoubleField(field uint32, v float64) {
	w.appendTag(field, wireFixed64)
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], math.Float64bits(v))
	w.buf = append(w.buf, tmp[:]...)
}

// writeBoolField writes a bool field, including v == false, for oneof
// members where presence is explicit.
func (w *protoWriter) writeBoolField(field uint32, v bool) {
	w.appendTag(field, wireVarint)
	if v {
		w.appendVarint(1)
	} else {
		w.appendVarint(0)
	}
}

// writeStringFieldAlways writes a length-delimited string field, including
// s == "", for oneof members where presence is explicit.
func (w *protoWriter) writeStringFieldAlways(field uint32, s string) {
	w.appendTag(field, wireBytes)
	w.appendVarint(uint64(len(s)))
	w.buf = append(w.buf, s...)
}

// --- Perfetto-specific message builders ---

// buildDebugAnnotation builds a DebugAnnotation submessage.
// Field numbers: name=10, string_value=6, int_value=4, double_value=5, bool_value=2
// The value is a proto oneof, so presence is explicit: zero values (0, false,
// "") must still be written or the annotation shows no value in Perfetto UI.
func buildDebugAnnotation(name string, value interface{}) []byte {
	var w protoWriter
	w.writeStringField(10, name)
	switch v := value.(type) {
	case string:
		w.writeStringFieldAlways(6, v)
	case int64:
		w.writeInt64Field(4, v)
	case int:
		w.writeInt64Field(4, int64(v))
	case float64:
		w.writeDoubleField(5, v)
	case bool:
		w.writeBoolField(2, v)
	default:
		// Convert to string as fallback
		if s, ok := value.(interface{ String() string }); ok {
			w.writeStringFieldAlways(6, s.String())
		}
	}
	return w.bytes()
}

// buildProcessDescriptor builds a ProcessDescriptor submessage.
// Field numbers: pid=1, process_name=6
func buildProcessDescriptor(pid int32, name string) []byte {
	var w protoWriter
	w.writeVarintField(1, uint64(pid))
	w.writeStringField(6, name)
	return w.bytes()
}

// buildThreadDescriptor builds a ThreadDescriptor submessage.
// Field numbers: pid=1, tid=2, thread_name=5
func buildThreadDescriptor(pid, tid int32, name string) []byte {
	var w protoWriter
	w.writeVarintField(1, uint64(pid))
	w.writeVarintField(2, uint64(tid))
	w.writeStringField(5, name)
	return w.bytes()
}

// buildTrackDescriptor builds a TrackDescriptor submessage.
// Field numbers: uuid=1, parent_uuid=5, name=2, process=3, thread=4
func buildTrackDescriptor(uuid uint64, parentUUID uint64, name string, process []byte, thread []byte) []byte {
	var w protoWriter
	w.writeVarintField(1, uuid)
	w.writeStringField(2, name)
	if len(process) > 0 {
		w.writeBytesField(3, process)
	}
	if len(thread) > 0 {
		w.writeBytesField(4, thread)
	}
	if parentUUID != 0 {
		w.writeVarintField(5, parentUUID)
	}
	return w.bytes()
}

// buildTrackEvent builds a TrackEvent submessage.
// Field numbers: type=9, track_uuid=11, name=23, debug_annotations=4
func buildTrackEvent(eventType uint64, trackUUID uint64, name string, annotations [][]byte) []byte {
	var w protoWriter
	w.writeVarintField(9, eventType)
	w.writeVarintField(11, trackUUID)
	w.writeStringField(23, name)
	for _, ann := range annotations {
		w.writeBytesField(4, ann)
	}
	return w.bytes()
}

// buildTracePacket builds a TracePacket with a TrackEvent.
// Field numbers: timestamp=8, trusted_packet_sequence_id=10, track_event=11, track_descriptor=60
func buildTracePacketEvent(timestampNs uint64, seqID uint32, trackEvent []byte) []byte {
	var w protoWriter
	w.writeVarintFieldAlways(8, timestampNs)
	w.writeVarintField(10, uint64(seqID))
	w.writeBytesField(11, trackEvent)
	return w.bytes()
}

// buildTracePacketDescriptor builds a TracePacket with a TrackDescriptor.
func buildTracePacketDescriptor(seqID uint32, descriptor []byte) []byte {
	var w protoWriter
	w.writeVarintField(10, uint64(seqID))
	w.writeBytesField(60, descriptor)
	return w.bytes()
}

// wrapTracePacket wraps a TracePacket in a Trace message (field 1, repeated).
func wrapTracePacket(packet []byte) []byte {
	var w protoWriter
	w.writeBytesField(1, packet)
	return w.bytes()
}
