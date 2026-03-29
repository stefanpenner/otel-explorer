// Command record-demo records an asciinema v2 .cast file by driving
// ote on a properly-sized pty with scripted keystrokes.
//
// Usage:
//
//	go run ./cmd/record-demo [output.cast]
//	svg-term --in output.cast --out docs/demo.svg --window --no-cursor
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/creack/pty"
)

const (
	cols = 120
	rows = 40
	url  = "https://github.com/stefanpenner/gha-analyzer/pull/44"
)

// castHeader is the asciicast v2 header.
type castHeader struct {
	Version   int               `json:"version"`
	Width     int               `json:"width"`
	Height    int               `json:"height"`
	Timestamp int64             `json:"timestamp"`
	Env       map[string]string `json:"env"`
}

// castEvent is a single output event: [time, "o", data].
type castEvent struct {
	Time float64
	Data string
}

func (e castEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal([3]any{e.Time, "o", e.Data})
}

// recorder manages pty I/O and cast file output. A single background
// goroutine continuously reads from the pty into a buffer. The drain
// method flushes whatever has accumulated.
type recorder struct {
	ptmx  *os.File
	start time.Time
	out   *os.File

	mu  sync.Mutex
	buf []byte
}

func newRecorder(ptmx, out *os.File) *recorder {
	r := &recorder{ptmx: ptmx, start: time.Now(), out: out}
	go r.readLoop()
	return r
}

// readLoop runs in a goroutine, continuously reading pty output.
func (r *recorder) readLoop() {
	buf := make([]byte, 65536)
	for {
		n, err := r.ptmx.Read(buf)
		if n > 0 {
			r.mu.Lock()
			r.buf = append(r.buf, buf[:n]...)
			r.mu.Unlock()
		}
		if err != nil {
			return
		}
	}
}

// drain flushes accumulated output to the cast file.
func (r *recorder) drain() {
	r.mu.Lock()
	if len(r.buf) == 0 {
		r.mu.Unlock()
		return
	}
	data := r.buf
	r.buf = nil
	r.mu.Unlock()

	evt := castEvent{
		Time: time.Since(r.start).Seconds(),
		Data: string(data),
	}
	out, _ := json.Marshal(evt)
	fmt.Fprintln(r.out, string(out))
}

// send writes a keystroke to the pty, pauses, then drains output.
func (r *recorder) send(key string, pause time.Duration) {
	_, _ = r.ptmx.Write([]byte(key))
	time.Sleep(pause)
	r.drain()
}

// wait pauses then drains output.
func (r *recorder) wait(d time.Duration) {
	time.Sleep(d)
	r.drain()
}

func main() {
	output := "docs/demo.cast"
	if len(os.Args) > 1 {
		output = os.Args[1]
	}

	// Create cast file
	f, err := os.Create(output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create %s: %v\n", output, err)
		os.Exit(1)
	}

	// Write header
	hdr := castHeader{
		Version:   2,
		Width:     cols,
		Height:    rows,
		Timestamp: time.Now().Unix(),
		Env:       map[string]string{"TERM": "xterm-256color", "SHELL": "/bin/zsh"},
	}
	hdrBytes, _ := json.Marshal(hdr)
	fmt.Fprintln(f, string(hdrBytes))

	// Spawn ote on a pty at the correct size
	cmd := exec.Command("ote", url)
	cmd.Env = append(os.Environ(),
		"TERM=xterm-256color",
		fmt.Sprintf("COLUMNS=%d", cols),
		fmt.Sprintf("LINES=%d", rows),
	)

	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: rows, Cols: cols})
	if err != nil {
		fmt.Fprintf(os.Stderr, "pty start: %v\n", err)
		os.Exit(1)
	}
	defer ptmx.Close()

	rec := newRecorder(ptmx, f)

	// Wait for TUI to load
	for range 50 {
		time.Sleep(200 * time.Millisecond)
		rec.drain()
		info, _ := os.Stat(output)
		if info != nil && info.Size() > 1000 {
			break
		}
	}
	rec.wait(1 * time.Second)

	// --- Scripted interaction ---

	// Navigate down through the tree
	for range 3 {
		rec.send("j", 200*time.Millisecond)
	}

	// Expand a node
	rec.send("\r", 600*time.Millisecond)

	// Navigate into children
	for range 3 {
		rec.send("j", 200*time.Millisecond)
	}

	// Expand another node
	rec.send("\r", 600*time.Millisecond)

	// Navigate down to see steps
	for range 4 {
		rec.send("j", 200*time.Millisecond)
	}

	// Pause to show the tree + timeline
	rec.wait(1500 * time.Millisecond)

	// Open detail/inspector view
	rec.send("i", 1500*time.Millisecond)

	// Navigate the inspector
	for range 3 {
		rec.send("j", 200*time.Millisecond)
	}

	// Expand an inspector section
	rec.send("\r", 500*time.Millisecond)
	rec.send("j", 200*time.Millisecond)
	rec.send("j", 200*time.Millisecond)

	// Show the detail view
	rec.wait(1500 * time.Millisecond)

	// Close the inspector
	rec.send("q", 500*time.Millisecond)

	// Demo search: type /build
	rec.send("/", 300*time.Millisecond)
	for _, ch := range "build" {
		rec.send(string(ch), 100*time.Millisecond)
	}
	rec.wait(1 * time.Second)

	// Exit search (Escape), keeping filter
	rec.send("\x1b", 1*time.Second)

	// Clear search (Escape)
	rec.send("\x1b", 500*time.Millisecond)

	// Final pause on the full view
	rec.wait(1500 * time.Millisecond)

	// Quit
	rec.send("q", 500*time.Millisecond)

	// Wait for process to exit
	_ = cmd.Wait()
	f.Close()

	// Trim dead time at the start — collapse everything before the TUI
	// appears into a single short frame so the demo starts immediately.
	trimLeadTime(output)

	fmt.Printf("Recorded to %s\n", output)
}

// trimLeadTime rewrites the cast file so that all frames before the TUI
// appears are collapsed to a brief 0.5s window, removing the loading delay.
func trimLeadTime(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 2 {
		return
	}

	// Find the first frame containing the TUI chrome (╭ border)
	tuiFrame := -1
	tuiTime := 0.0
	for i := 1; i < len(lines); i++ {
		if strings.Contains(lines[i], "╭") {
			var evt [3]any
			if err := json.Unmarshal([]byte(lines[i]), &evt); err == nil {
				tuiTime, _ = evt[0].(float64)
				tuiFrame = i
			}
			break
		}
	}
	if tuiFrame < 0 || tuiTime < 1.0 {
		return // no significant lead time to trim
	}

	// Rewrite: keep header, then shift all timestamps so the TUI frame
	// starts at 0.5s. Frames before the TUI frame get compressed into 0-0.5s.
	shift := tuiTime - 0.5
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	fmt.Fprintln(w, lines[0]) // header unchanged

	for i := 1; i < len(lines); i++ {
		var evt [3]any
		if err := json.Unmarshal([]byte(lines[i]), &evt); err != nil {
			fmt.Fprintln(w, lines[i])
			continue
		}
		t, _ := evt[0].(float64)
		if i < tuiFrame {
			// Compress pre-TUI frames into 0-0.5s window
			ratio := t / tuiTime
			evt[0] = ratio * 0.5
		} else {
			evt[0] = t - shift
		}
		out, _ := json.Marshal(evt)
		fmt.Fprintln(w, string(out))
	}
	w.Flush()
}
