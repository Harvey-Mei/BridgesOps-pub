package modelcheck

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"strings"
	"testing"
	"unicode/utf8"
)

func testPelicanSVG(nonce string) string {
	return fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 640 360"><circle cx="200" cy="240" r="55"><animateTransform attributeName="transform" type="rotate" from="0 200 240" to="360 200 240" dur="4s" repeatCount="indefinite"/></circle><text x="400" y="340">%s</text></svg>`, nonce)
}

func TestPelicanChallengeAndSafePreview(t *testing.T) {
	c, err := buildPelicanChallenge(zeroReader{})
	if err != nil {
		t.Fatal(err)
	}
	svg := testPelicanSVG(c.expected)
	if passed, why := c.evaluate(svg); !passed {
		t.Fatal(why)
	}
	if passed, _ := c.evaluate(testPelicanSVG("old-nonce")); passed {
		t.Fatal("accepted stale nonce")
	}
	if passed, _ := c.evaluate(strings.ReplaceAll(svg, "<animateTransform attributeName=\"transform\" type=\"rotate\" from=\"0 200 240\" to=\"360 200 240\" dur=\"4s\" repeatCount=\"indefinite\"/>", "")); passed {
		t.Fatal("accepted missing animation")
	}
	for name, raw := range map[string]string{
		"css_comment_duration":  strings.Replace(svg, "</svg>", `<style>circle{animation:spin/**/.001s 1s infinite}</style></svg>`, 1),
		"css_quoted_duration":   strings.Replace(svg, "</svg>", `<style>circle{animation:"spin".001s 1s infinite}</style></svg>`, 1),
		"css_number_in_name":    strings.Replace(svg, "</svg>", `<style>circle{animation:spin1s .001s 1s infinite}</style></svg>`, 1),
		"split_css":             strings.Replace(svg, "</svg>", `<style>@im<![CDATA[port]]> "//example.invalid/x.css";</style></svg>`, 1),
		"css_rapid":             strings.Replace(svg, "</svg>", `<style>circle{animation:spin .001s infinite}</style></svg>`, 1),
		"css_signed_duration":   strings.Replace(svg, "</svg>", `<style>circle{animation:spin +.001s 1s infinite}</style></svg>`, 1),
		"css_exponent_duration": strings.Replace(svg, "</svg>", `<style>circle{animation:spin 1e-3s 1s infinite}</style></svg>`, 1),
		"css_computed_duration": strings.Replace(svg, "</svg>", `<style>circle{animation:spin min(.001s) 1s infinite}</style></svg>`, 1),
		"css_many_names":        strings.Replace(svg, "</svg>", `<style>circle{animation-name:a,b,c,d,e;animation-duration:1s}</style></svg>`, 1),
		"css_filter":            strings.Replace(svg, "</svg>", `<style>circle{filter:blur(1000px)}</style></svg>`, 1),
		"use_expansion":         strings.Replace(svg, "</svg>", `<g id="a"><use href="#a"/></g></svg>`, 1),
		"script":                strings.Replace(svg, "</svg>", `<script>alert(1)</script></svg>`, 1),
		"event":                 strings.Replace(svg, "<circle", `<circle onload="alert(1)"`, 1),
		"foreign":               strings.Replace(svg, "</svg>", `<foreignObject><div>unsafe</div></foreignObject></svg>`, 1),
		"external_use":          strings.Replace(svg, "</svg>", `<use href="https://example.invalid/x.svg#x"/></svg>`, 1),
		"external_css":          strings.Replace(svg, "</svg>", `<style>@import 'https://example.invalid/x.css';</style></svg>`, 1),
		"escaped_css":           strings.Replace(svg, "</svg>", `<style>@\69mport 'x';</style></svg>`, 1),
		"animated_href":         strings.Replace(svg, "</svg>", `<animate attributeName="href" values="https://example.invalid"/></svg>`, 1),
		"entity":                `<!DOCTYPE svg [<!ENTITY p SYSTEM "file:///etc/passwd">]>` + svg,
		"processing":            `<?xml-stylesheet href="https://example.invalid"?>` + svg,
		"huge":                  strings.Repeat(" ", MaxPelicanSVGBytes) + svg,
		"elements":              strings.Replace(svg, "</svg>", strings.Repeat("<rect/>", 2001)+"</svg>", 1),
		"depth":                 strings.Replace(svg, "</svg>", strings.Repeat("<g>", 49)+strings.Repeat("</g>", 49)+"</svg>", 1),
		"rapid_motion":          strings.Replace(svg, `dur="4s"`, `dur="0.001s"`, 1),
		"root_tail":             svg + svg,
		"broken":                strings.TrimSuffix(svg, "</svg>"),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := SafePelicanSVG(raw); err == nil {
				t.Fatal("unsafe SVG accepted")
			}
		})
	}
}

func TestCandyMinimumMatchesEnumeratedDraws(t *testing.T) {
	cases := [][6]int{{7, 9, 8, 7, 6, 4}, {1, 1, 1, 1, 1, 1}, {4, 4, 4, 4, 4, 4}, {1, 3, 2, 2, 1, 3}, {3, 2, 1, 1, 3, 2}}
	for _, c := range cases {
		// Enumerate actual color/shape draws, independently of the production
		// shortcut over the four maximal failing sets.
		bad := map[[2]int]bool{}
		var draw [6]int
		var enumerate func(int)
		enumerate = func(i int) {
			if i == 6 {
				if !(draw[0] > 0 && draw[4] > 0) && !(draw[1] > 0 && draw[3] > 0) {
					bad[[2]int{draw[0] + draw[1] + draw[2], draw[3] + draw[4] + draw[5]}] = true
				}
				return
			}
			for n := 0; n <= c[i]; n++ {
				draw[i] = n
				enumerate(i + 1)
			}
		}
		enumerate(0)
		want := 10000
		for r := 0; r <= c[0]+c[1]+c[2]; r++ {
			for s := 0; s <= c[3]+c[4]+c[5]; s++ {
				if !bad[[2]int{r, s}] && r+s < want {
					want = r + s
				}
			}
		}
		if got := candyMinimum(c); got != want {
			t.Fatalf("counts %v got %d want %d", c, got, want)
		}
	}
	c, err := buildCandyChallenge(zeroReader{})
	if err != nil {
		t.Fatal(err)
	}
	if passed, why := c.evaluate(c.expected); !passed {
		t.Fatal(why)
	}
	if passed, _ := c.evaluate(`{"nonce":"000000000000","answer":21}`); passed {
		t.Fatal("accepted the reference site's fixed answer for another question")
	}
}

type pelicanRunner struct{ fakeRunner }

func (r *pelicanRunner) RunProbe(_ context.Context, target TargetDescriptor, request ProbeRequest) (ProbeResponse, error) {
	r.probeCalls++
	text := `{"nonce":"000000000000","answer":14}`
	if request.Kind == PelicanProbeKind {
		text = strings.Replace(testPelicanSVG("000000000000"), "</svg>", "<desc>"+strings.Repeat("evidence ", 400)+"</desc></svg>", 1)
	}
	return ProbeResponse{Text: text, ActualModel: target.ModelName}, nil
}
func TestPelicanRunRetainsTwelveResultsWithoutResettingBudget(t *testing.T) {
	store := openModelCheckTestStore(t)
	runner := &pelicanRunner{fakeRunner: fakeRunner{targets: []TargetDescriptor{{ChannelID: 3, ChannelName: "test", GroupName: "gpt", ModelName: "gpt-test", App: storage.GatewayAppCodex}}}}
	service := New(Options{Store: store, Runner: runner, Random: zeroReader{}})
	targets, err := service.SyncTargets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.ModelCheckConfig{ProbesPerTarget: 2, RequestBudget24h: 26, TimeoutSeconds: 30}
	for i := 0; i < 13; i++ {
		run, err := service.RunTarget(context.Background(), targets[0].ID, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != storage.ModelCheckRunPassed || run.RequestCount != 2 {
			t.Fatalf("run %d: %s %s", i, run.Status, run.AnomalyReasons)
		}
	}
	latest, err := service.LatestRuns()
	if err != nil {
		t.Fatal(err)
	}
	if len(latest) != 12 {
		t.Fatalf("results %d", len(latest))
	}
	for _, run := range latest {
		var ev []probeEvidence
		if err := json.Unmarshal([]byte(run.EvidenceJSON), &ev); err != nil {
			t.Fatal(err)
		}
		if len(ev) != 2 || len(ev[1].ArtifactSVG) <= maxEvidenceText || utf8.RuneCountInString(ev[1].Response) > maxEvidenceText+1 {
			t.Fatal("SVG was truncated or evidence exceeded its bound")
		}
	}
	used, err := service.RequestsUsed24h()
	if err != nil || used != 26 {
		t.Fatalf("budget used %d %v", used, err)
	}
	if _, err := service.RunTarget(context.Background(), targets[0].ID, cfg); err != ErrRequestBudgetExceeded {
		t.Fatalf("budget was reset: %v", err)
	}
}
