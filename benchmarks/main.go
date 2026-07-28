// Command bench runs syzy's curated benchmark scenarios and renders a
// markdown report at benchmarks/README.md plus a JSON snapshot under
// benchmarks/results/.
//
// Run from repo root:
//
//	go run ./benchmarks
//	./benchmarks/run.sh                # convenience wrapper
//	go run ./benchmarks -benchtime 5x  # quick development run
//
// Add scenarios by appending to the scenarios slice in scenarios.go.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"
)

// Scenario describes one benchmark we want in the report.
type Scenario struct {
	Name        string `json:"name"`
	Group       string `json:"group"`
	Bench       string `json:"bench"`
	Package     string `json:"package"`
	Description string `json:"description,omitempty"`
	CompareWith string `json:"compare_with,omitempty"`
	Benchtime   string `json:"benchtime,omitempty"`
	Count       int    `json:"count,omitempty"`
}

// Result is the parsed median across multiple bench runs.
type Result struct {
	Scenario    string  `json:"scenario"`
	Group       string  `json:"group"`
	NsPerOp     float64 `json:"ns_per_op"`
	BPerOp      float64 `json:"bytes_per_op"`
	AllocsPerOp float64 `json:"allocs_per_op"`
	Iterations  int     `json:"iterations"`
	NumRuns     int     `json:"num_runs"`
}

// Report is the top-level structure persisted as JSON and fed to the
// markdown template.
type Report struct {
	Version     int        `json:"version"`
	GeneratedAt time.Time  `json:"generated_at"`
	Benchtime   string     `json:"benchtime"`
	Count       int        `json:"count"`
	GitCommit   string     `json:"git_commit"`
	GitBranch   string     `json:"git_branch"`
	GoVersion   string     `json:"go_version"`
	GOOS        string     `json:"goos"`
	GOARCH      string     `json:"goarch"`
	KernelVer   string     `json:"kernel_version,omitempty"`
	CPUModel    string     `json:"cpu_model,omitempty"`
	NumCPU      int        `json:"num_cpu"`
	Scenarios   []Scenario `json:"scenarios"`
	Results     []Result   `json:"results"`
}

// findResult returns the result for scenarioName, or zero + false.
func (r *Report) findResult(scenarioName string) (Result, bool) {
	for _, x := range r.Results {
		if x.Scenario == scenarioName {
			return x, true
		}
	}
	return Result{}, false
}

// benchLineRe matches one go-test bench result line.
//
//	BenchmarkName-N    iters    ns ns/op   [B B/op]   [allocs allocs/op]
//
// The BenchmarkName-N prefix is optional: the benchmark's own stderr
// logging (merged into stdout by go test) can land between the name
// and the numbers, splitting them across lines. -bench is pinned to a
// single benchmark per run, so an orphaned result line is unambiguous.
var benchLineRe = regexp.MustCompile(`^(?:Benchmark\S+\s+)?(\d+)\s+([\d.]+)\s+ns/op(?:\s+(\d+)\s+B/op)?(?:\s+(\d+)\s+allocs/op)?`)

func runBench(sc Scenario, benchtime string, count int) (Result, error) {
	if sc.Benchtime != "" {
		benchtime = sc.Benchtime
	}
	if sc.Count > 0 {
		count = sc.Count
	}
	cmd := exec.Command("go", "test",
		"-run", "^$",
		"-bench", "^"+regexp.QuoteMeta(sc.Bench)+"$",
		"-benchmem",
		"-benchtime", benchtime,
		"-count", strconv.Itoa(count),
		sc.Package,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return Result{}, fmt.Errorf("%s: %v\n%s", sc.Bench, err, out)
	}
	return parseBenchOutput(sc, string(out))
}

func parseBenchOutput(sc Scenario, out string) (Result, error) {
	var nsRuns, bRuns, aRuns []float64
	var iters int
	for _, line := range strings.Split(out, "\n") {
		m := benchLineRe.FindStringSubmatch(strings.TrimSpace(line))
		if m == nil {
			continue
		}
		ns, _ := strconv.ParseFloat(m[2], 64)
		nsRuns = append(nsRuns, ns)
		if m[3] != "" {
			b, _ := strconv.ParseFloat(m[3], 64)
			bRuns = append(bRuns, b)
		}
		if m[4] != "" {
			a, _ := strconv.ParseFloat(m[4], 64)
			aRuns = append(aRuns, a)
		}
		iters, _ = strconv.Atoi(m[1])
	}
	if len(nsRuns) == 0 {
		return Result{}, fmt.Errorf("no benchmark lines parsed for %s; output:\n%s", sc.Bench, truncate(out, 4000))
	}
	// Drop the first run as warmup if we have at least 3.
	dropFirst := len(nsRuns) >= 3
	res := Result{
		Scenario:   sc.Name,
		Group:      sc.Group,
		NsPerOp:    median(nsRuns, dropFirst),
		Iterations: iters,
		NumRuns:    len(nsRuns),
	}
	if len(bRuns) > 0 {
		res.BPerOp = median(bRuns, dropFirst)
	}
	if len(aRuns) > 0 {
		res.AllocsPerOp = median(aRuns, dropFirst)
	}
	return res, nil
}

func median(xs []float64, dropFirst bool) float64 {
	if dropFirst && len(xs) > 1 {
		xs = xs[1:]
	}
	if len(xs) == 0 {
		return 0
	}
	cp := append([]float64(nil), xs...)
	sort.Float64s(cp)
	return cp[len(cp)/2]
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...(truncated)"
}

// captureHostInfo fills the host-identifying fields of r.
func captureHostInfo(r *Report) {
	r.GoVersion = runtime.Version()
	r.GOOS = runtime.GOOS
	r.GOARCH = runtime.GOARCH
	r.NumCPU = runtime.NumCPU()
	if runtime.GOOS == "linux" {
		if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if strings.HasPrefix(line, "model name") {
					parts := strings.SplitN(line, ":", 2)
					if len(parts) == 2 {
						r.CPUModel = strings.TrimSpace(parts[1])
						break
					}
				}
			}
		}
		if out, err := exec.Command("uname", "-r").Output(); err == nil {
			r.KernelVer = strings.TrimSpace(string(out))
		}
	}
	if out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output(); err == nil {
		r.GitCommit = strings.TrimSpace(string(out))
	}
	if out, err := exec.Command("git", "rev-parse", "--abbrev-ref", "HEAD").Output(); err == nil {
		r.GitBranch = strings.TrimSpace(string(out))
	}
}

func main() {
	var (
		benchtime = flag.String("benchtime", "50000x", "go test -benchtime")
		count     = flag.Int("count", 5, "number of runs per scenario (median is reported)")
		dir       = flag.String("dir", "benchmarks", "output directory")
		filter    = flag.String("filter", "", "regex to limit scenarios by Name (case-insensitive)")
		dryRun    = flag.Bool("dry-run", false, "render without running anything")
		report    = flag.Bool("report", true, "write README.md and latest.json (disable for smoke runs)")
	)
	flag.Parse()

	r := &Report{
		Version:     1,
		GeneratedAt: time.Now().UTC(),
		Benchtime:   *benchtime,
		Count:       *count,
		Scenarios:   scenarios,
	}
	captureHostInfo(r)

	var re *regexp.Regexp
	if *filter != "" {
		re = regexp.MustCompile("(?i)" + *filter)
	}

	failed := 0
	if !*dryRun {
		for _, sc := range scenarios {
			if re != nil && !re.MatchString(sc.Name) {
				continue
			}
			fmt.Fprintf(os.Stderr, "running %-50s ", sc.Bench)
			res, err := runBench(sc, *benchtime, *count)
			if err != nil {
				fmt.Fprintf(os.Stderr, "FAIL\n%v\n", err)
				failed++
				continue
			}
			fmt.Fprintf(os.Stderr, "%9.0f ns/op  %3.0f allocs/op  %4.0f B/op\n",
				res.NsPerOp, res.AllocsPerOp, res.BPerOp)
			r.Results = append(r.Results, res)
		}
	}
	// A failed scenario (deleted bench, moved package) must fail the run,
	// not silently vanish from the report.
	if failed > 0 {
		fmt.Fprintf(os.Stderr, "\n%d scenario(s) failed; not writing report\n", failed)
		os.Exit(1)
	}
	// A filtered run covers a subset; writing it would silently replace
	// the tracked full report with a partial one.
	if re != nil {
		fmt.Fprintf(os.Stderr, "\nfiltered run; not writing report\n")
		return
	}
	if !*report {
		fmt.Fprintf(os.Stderr, "\n-report=false; not writing report\n")
		return
	}

	if err := saveJSON(*dir, r); err != nil {
		fmt.Fprintf(os.Stderr, "save JSON: %v\n", err)
		os.Exit(1)
	}
	if err := renderReadme(*dir, r); err != nil {
		fmt.Fprintf(os.Stderr, "render README: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "\nwrote %s\n", filepath.Join(*dir, "README.md"))
}

// saveJSON writes the report to benchmarks/latest.json. Each run
// overwrites the previous snapshot — historical comparison is the
// repo's git history (git log -p benchmarks/latest.json), not a
// per-run file zoo.
func saveJSON(dir string, r *Report) error {
	path := filepath.Join(dir, "latest.json")
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// fmtNs formats a ns/op float as "12,345" with thousands separators.
// Returns "—" for zero (missing data).
func fmtNs(v float64) string {
	if v == 0 {
		return "—"
	}
	return commafy(int64(v + 0.5))
}

// fmtOpsPerSec converts ns/op to ops/sec, rendered with thousands
// separators. Same workload as the ns/op column, just inverted to
// the unit that's intuitive for "how many of these per second".
// Returns "—" for zero (missing data).
func fmtOpsPerSec(nsPerOp float64) string {
	if nsPerOp == 0 {
		return "—"
	}
	ops := 1e9 / nsPerOp
	return commafy(int64(ops + 0.5))
}

func commafy(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := false
	if strings.HasPrefix(s, "-") {
		neg = true
		s = s[1:]
	}
	out := make([]byte, 0, len(s)+len(s)/3)
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}

// fmtDelta returns a delta string vs baseline, e.g., "+531 ns (5.1%)".
// If baseline is zero, returns "—".
func fmtDelta(value, baseline float64) string {
	if baseline == 0 {
		return "—"
	}
	delta := value - baseline
	pct := 100.0 * delta / baseline
	sign := "+"
	if delta < 0 {
		sign = "−"
		delta = -delta
		pct = -pct
	}
	return fmt.Sprintf("%s%s ns (%.1f%%)", sign, commafy(int64(delta+0.5)), pct)
}

// groupedResults returns results grouped by Group, in insertion order
// (Scenario list order). Used by the template.
type groupedSection struct {
	Group   string
	Notes   string
	Results []groupedResult
}

type groupedResult struct {
	Scenario    string
	Description string
	NsPerOp     float64
	BPerOp      float64
	AllocsPerOp float64
	Delta       string // "" if no baseline
}

// groupSections walks scenarios in order and bundles their Results
// into sections keyed by Group, applying delta computations against
// CompareWith targets.
func (r *Report) groupSections() []groupedSection {
	groupOrder := []string{}
	groupSet := map[string]int{}
	for _, sc := range r.Scenarios {
		if _, ok := groupSet[sc.Group]; !ok {
			groupSet[sc.Group] = len(groupOrder)
			groupOrder = append(groupOrder, sc.Group)
		}
	}
	sections := make([]groupedSection, len(groupOrder))
	for i, g := range groupOrder {
		sections[i] = groupedSection{Group: g}
		if note, ok := groupNotes[g]; ok {
			sections[i].Notes = note
		}
	}
	for _, sc := range r.Scenarios {
		res, ok := r.findResult(sc.Name)
		if !ok {
			continue
		}
		gr := groupedResult{
			Scenario:    sc.Name,
			Description: sc.Description,
			NsPerOp:     res.NsPerOp,
			BPerOp:      res.BPerOp,
			AllocsPerOp: res.AllocsPerOp,
		}
		if sc.CompareWith != "" {
			if base, ok := r.findResult(sc.CompareWith); ok {
				gr.Delta = fmtDelta(res.NsPerOp, base.NsPerOp)
			}
		}
		idx := groupSet[sc.Group]
		sections[idx].Results = append(sections[idx].Results, gr)
	}
	return sections
}

func renderReadme(dir string, r *Report) error {
	tmpl := template.Must(template.New("readme").Funcs(template.FuncMap{
		"fmtns":    fmtNs,
		"fmtops":   fmtOpsPerSec,
		"sections": func() []groupedSection { return r.groupSections() },
		"date": func(t time.Time) string {
			return t.Format("2006-01-02 15:04:05 UTC")
		},
		"int": func(f float64) int { return int(f + 0.5) },
	}).Parse(readmeTemplate))
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, r); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "README.md"), buf.Bytes(), 0o644)
}

const readmeTemplate = `# syzy benchmarks

Regenerate with ` + "`./benchmarks/run.sh`" + `.

{{ if .CPUModel }}{{ .CPUModel }} · {{ end }}{{ .GoVersion }} · {{ .GOOS }}/{{ .GOARCH }}{{ if .KernelVer }} · kernel {{ .KernelVer }}{{ end }} · ` + "`{{ .GitCommit }}`" + ` · ` + "`-benchtime={{ .Benchtime }} -count={{ .Count }}`" + ` · {{ .GeneratedAt | date }}
{{ range sections }}
## {{ .Group }}
{{ if .Notes }}
{{ .Notes }}
{{ end }}
| Scenario | ns/op | ops/sec | Δ vs baseline | allocs/op | B/op |
|---|---:|---:|---:|---:|---:|
{{- range .Results }}
| {{ .Scenario }} | {{ fmtns .NsPerOp }} | {{ fmtops .NsPerOp }} | {{ if .Delta }}{{ .Delta }}{{ else }}—{{ end }} | {{ int .AllocsPerOp }} | {{ int .BPerOp }} |
{{- end }}
{{ end }}

Numbers are medians across runs (first dropped as warmup). Microbenchmarks; real-world latency depends on disk speed, fsync mode, and host load. ` + "`Δ vs baseline`" + ` is the scenario's overhead vs its declared comparison row (the matching stock-SQLite row at the same batch size).
`
