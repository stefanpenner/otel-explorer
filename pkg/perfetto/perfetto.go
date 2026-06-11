package perfetto

import (
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
	"go.opentelemetry.io/otel/sdk/trace"
)

// trackKey identifies a track by its process/thread pair.
type trackKey struct {
	pid, tid int
}

// trackState holds descriptor info for a track being built.
type trackState struct {
	uuid       uint64
	parentUUID uint64
	name       string
	isProcess  bool // true = ProcessDescriptor, false = ThreadDescriptor
	pid, tid   int
}

func makeUUID(parts ...interface{}) uint64 {
	h := fnv.New64a()
	fmt.Fprint(h, parts...)
	return h.Sum64()
}

// spanEvent holds the data needed to emit a TracePacket for one span or legacy event.
type spanEvent struct {
	trackUUID   uint64
	startNs     uint64
	endNs       uint64 // 0 for instants
	name        string
	annotations [][]byte
	isInstant   bool
}

func WriteTrace(w io.Writer, urlResults []analyzer.URLResult, combined analyzer.CombinedMetrics, traceEvents []analyzer.TraceEvent, globalEarliestTime int64, perfettoFile string, openInPerfetto bool, spans []trace.ReadOnlySpan) error {
	// Find earliest nanosecond timestamp across all sources
	earliestNs := globalEarliestTime * 1_000_000
	for _, s := range spans {
		ns := s.StartTime().UnixNano()
		if ns < earliestNs {
			earliestNs = ns
		}
	}
	for _, res := range urlResults {
		if res.EarliestTime != 0 {
			ns := res.EarliestTime * 1_000_000
			if ns < earliestNs {
				earliestNs = ns
			}
		}
	}

	// Track registry: collect all tracks before emitting
	processTracks := make(map[int]*trackState)  // pid -> state
	threadTracks := make(map[trackKey]*trackState) // (pid,tid) -> state

	getProcessTrack := func(pid int, name string) uint64 {
		if t, ok := processTracks[pid]; ok {
			if name != "" && t.name == "" {
				t.name = name
			}
			return t.uuid
		}
		uuid := makeUUID("process", pid)
		processTracks[pid] = &trackState{
			uuid: uuid, name: name, isProcess: true, pid: pid,
		}
		return uuid
	}

	getThreadTrack := func(pid, tid int, name string) uint64 {
		key := trackKey{pid, tid}
		if t, ok := threadTracks[key]; ok {
			if name != "" && t.name == "" {
				t.name = name
			}
			return t.uuid
		}
		parentUUID := getProcessTrack(pid, "")
		uuid := makeUUID("thread", pid, tid)
		threadTracks[key] = &trackState{
			uuid: uuid, parentUUID: parentUUID, name: name,
			pid: pid, tid: tid,
		}
		return uuid
	}

	// First pass: collect events and register tracks
	var events []spanEvent

	for _, s := range spans {
		attrs := make(map[string]interface{})
		for _, attr := range s.Attributes() {
			val := attr.Value.AsInterface()
			if str, ok := val.(string); ok {
				stripped := utils.StripANSI(str)
				if len(stripped) > 1000 {
					// Truncate on a rune boundary so the protobuf string
					// field stays valid UTF-8.
					cut := 1000
					for cut > 0 && !utf8.RuneStart(stripped[cut]) {
						cut--
					}
					stripped = stripped[:cut] + "..."
				}
				val = stripped
			}
			attrs[string(attr.Key)] = val
		}

		startNs := s.StartTime().UnixNano() - earliestNs
		endNs := s.EndTime().UnixNano() - earliestNs
		if startNs < 0 {
			startNs = 0
		}
		if endNs < startNs {
			endNs = startNs
		}

		spanType, _ := attrs["type"].(string)
		name := utils.StripANSI(s.Name())
		isMarker := spanType == "marker"

		if !isMarker && endNs <= startNs {
			endNs = startNs + 1_000_000 // 1ms minimum
		}

		// Build debug annotations
		var annotations [][]byte
		for k, v := range attrs {
			annotations = append(annotations, buildDebugAnnotation(k, v))
		}

		// Determine track (same pid/tid logic as before)
		pid := 1
		tid := 1
		trackName := ""

		switch spanType {
		case "workflow":
			if runID, ok := attrs["github.run_id"].(int64); ok {
				pid = int(runID % 2147483647)
				tid = 0 // workflow spans go on process track
				trackName = name
			}
		case "job":
			if runID, ok := attrs["github.run_id"].(int64); ok {
				pid = int(runID % 2147483647)
			}
			if jobID, ok := attrs["github.job_id"].(int64); ok {
				tid = int(jobID % 2147483647)
				trackName = "Job: " + name
			}
		case "step":
			if runID, ok := attrs["github.run_id"].(int64); ok {
				pid = int(runID % 2147483647)
			}
			parentID := s.Parent().SpanID()
			var tidVal uint32
			for i := 0; i < 8; i++ {
				tidVal = (tidVal << 8) | uint32(parentID[i])
			}
			tid = int(tidVal % 2147483647)
		case "marker":
			pid = 999
			tid = 2
		default:
			// Non-GHA spans (e.g. Bazel traces from artifacts)
			pid = 998
			parentID := s.Parent().SpanID()
			if parentID.IsValid() {
				var tidVal uint32
				for i := 0; i < 8; i++ {
					tidVal = (tidVal << 8) | uint32(parentID[i])
				}
				tid = int(tidVal % 2147483647)
			} else {
				spanID := s.SpanContext().SpanID()
				var tidVal uint32
				for i := 0; i < 8; i++ {
					tidVal = (tidVal << 8) | uint32(spanID[i])
				}
				tid = int(tidVal % 2147483647)
				trackName = name
			}
		}

		// Register tracks with names
		var trackUUID uint64
		switch {
		case spanType == "workflow" && tid == 0:
			trackUUID = getProcessTrack(pid, trackName)
		case isMarker:
			getProcessTrack(999, "GitHub Events")
			trackUUID = getThreadTrack(999, 2, "GitHub PR Events")
		default:
			if pid == 998 {
				processName := "Trace Artifacts"
				if n, ok := attrs["github.artifact_name"].(string); ok {
					processName = n
				}
				getProcessTrack(pid, processName)
			}
			if trackName != "" {
				trackUUID = getThreadTrack(pid, tid, trackName)
			} else {
				trackUUID = getThreadTrack(pid, tid, "")
			}
		}

		events = append(events, spanEvent{
			trackUUID:   trackUUID,
			startNs:     uint64(startNs),
			endNs:       uint64(endNs),
			name:        name,
			annotations: annotations,
			isInstant:   isMarker,
		})
	}

	// Process legacy TraceEvents (skip metadata, convert data events)
	for _, ev := range traceEvents {
		if ev.Ph == "M" {
			continue
		}

		ev.Name = utils.StripANSI(ev.Name)

		// Renormalize legacy events with url_index
		isLegacy := false
		if ev.Args != nil {
			if _, ok := ev.Args["url_index"]; ok {
				isLegacy = true
			}
		}
		if isLegacy && ev.Ts != 0 {
			eventURLIndex := 1
			if val, ok := ev.Args["url_index"].(int); ok {
				eventURLIndex = val
			} else if valFloat, ok := ev.Args["url_index"].(float64); ok {
				eventURLIndex = int(valFloat)
			}
			eventSource := ""
			if val, ok := ev.Args["source_url"].(string); ok {
				eventSource = val
			}

			var urlResult *analyzer.URLResult
			for i := range urlResults {
				if urlResults[i].URLIndex == eventURLIndex-1 || urlResults[i].DisplayURL == eventSource {
					urlResult = &urlResults[i]
					break
				}
			}

			if urlResult != nil {
				// ev.Ts is in microseconds relative to urlResult's earliestTime
				absoluteMs := ev.Ts/1000 + urlResult.EarliestTime
				ev.Ts = (absoluteMs - globalEarliestTime) * 1000
			}
		}
		if ev.Ts < 0 {
			ev.Ts = 0
		}

		// Convert microseconds to nanoseconds
		startNs := uint64(ev.Ts * 1000)
		endNs := startNs + uint64(ev.Dur*1000)

		// Build annotations
		var annotations [][]byte
		if ev.Args != nil {
			for k, v := range ev.Args {
				if str, ok := v.(string); ok {
					v = utils.StripANSI(str)
				}
				annotations = append(annotations, buildDebugAnnotation(k, v))
			}
		}

		// Register track
		if ev.Pid != 0 || ev.Tid != 0 {
			getProcessTrack(ev.Pid, "")
		}
		trackUUID := getThreadTrack(ev.Pid, ev.Tid, "")

		events = append(events, spanEvent{
			trackUUID:   trackUUID,
			startNs:     startNs,
			endNs:       endNs,
			name:        ev.Name,
			annotations: annotations,
			isInstant:   ev.Ph == "i",
		})
	}

	// Build output: descriptors first, then events
	seqID := uint32(1)
	var traceData []byte

	// Emit process track descriptors
	for _, t := range processTracks {
		proc := buildProcessDescriptor(int32(t.pid), t.name)
		desc := buildTrackDescriptor(t.uuid, 0, t.name, proc, nil)
		pkt := buildTracePacketDescriptor(seqID, desc)
		traceData = append(traceData, wrapTracePacket(pkt)...)
	}

	// Emit thread track descriptors
	for _, t := range threadTracks {
		thread := buildThreadDescriptor(int32(t.pid), int32(t.tid), t.name)
		desc := buildTrackDescriptor(t.uuid, t.parentUUID, t.name, nil, thread)
		pkt := buildTracePacketDescriptor(seqID, desc)
		traceData = append(traceData, wrapTracePacket(pkt)...)
	}

	// Emit events
	for _, ev := range events {
		if ev.isInstant {
			te := buildTrackEvent(typeInstant, ev.trackUUID, ev.name, ev.annotations)
			pkt := buildTracePacketEvent(ev.startNs, seqID, te)
			traceData = append(traceData, wrapTracePacket(pkt)...)
		} else {
			// SLICE_BEGIN with name and annotations
			te := buildTrackEvent(typeSliceBegin, ev.trackUUID, ev.name, ev.annotations)
			pkt := buildTracePacketEvent(ev.startNs, seqID, te)
			traceData = append(traceData, wrapTracePacket(pkt)...)

			// SLICE_END (no name or annotations needed)
			te = buildTrackEvent(typeSliceEnd, ev.trackUUID, "", nil)
			pkt = buildTracePacketEvent(ev.endNs, seqID, te)
			traceData = append(traceData, wrapTracePacket(pkt)...)
		}
	}

	if err := os.WriteFile(perfettoFile, traceData, 0o644); err != nil {
		return err
	}
	fmt.Fprintf(w, "\n Perfetto trace saved to: %s\n", perfettoFile)

	if openInPerfetto {
		return openTraceInPerfetto(w, perfettoFile)
	}
	return nil
}

func openTraceInPerfetto(w io.Writer, traceFile string) error {
	scriptName := "open_trace_in_ui"
	scriptURL := "https://raw.githubusercontent.com/google/perfetto/main/tools/open_trace_in_ui"
	scriptPath := filepath.Join(os.TempDir(), scriptName)

	if _, err := os.Stat(scriptPath); err != nil {
		fmt.Fprintln(w, "\n Opening trace in Perfetto UI...")
		fmt.Fprintln(w, " Downloading open_trace_in_ui from Perfetto...")
		if err := exec.Command("curl", "-L", "-o", scriptPath, scriptURL).Run(); err != nil {
			return errors.Wrapf(err, "failed to download %s", scriptName)
		}
		if err := exec.Command("chmod", "+x", scriptPath).Run(); err != nil {
			return errors.Wrapf(err, "failed to make %s executable", scriptName)
		}
	} else {
		fmt.Fprintf(w, "\n Using existing script: %s\n", scriptPath)
	}

	fmt.Fprintf(w, " Opening %s in Perfetto UI...\n", traceFile)
	cmd := exec.Command(scriptPath, traceFile)
	cmd.Env = append(os.Environ(), "PYTHONIOENCODING=utf-8")
	cmd.Stdout = w
	cmd.Stderr = w
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(w, " Failed to open trace in Perfetto\n")
		fmt.Fprintln(w, " You can manually open the trace at: https://ui.perfetto.dev")
		fmt.Fprintf(w, "   Then click \"Open trace file\" and select: %s\n", traceFile)
		return nil
	}
	fmt.Fprintln(w, "Trace opened successfully in Perfetto UI!")
	return nil
}
