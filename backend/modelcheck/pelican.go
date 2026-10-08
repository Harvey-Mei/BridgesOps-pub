package modelcheck

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
)

const PelicanProbeKind = "pelican_svg"
const CandyProbeKind = "candy_shapes"
const MaxPelicanSVGBytes = 96 << 10

func buildPelicanChallenge(random io.Reader) (challenge, error) {
	scenes := []string{"海边的木栈道", "湖畔的春日花园", "秋天的森林小径", "夕阳下的乡间公路", "雪山脚下的小镇", "风车旁的草坡"}
	index, err := randomInt(random, len(scenes))
	if err != nil {
		return challenge{}, err
	}
	nonce, err := randomNonce(random, 6)
	if err != nil {
		return challenge{}, err
	}
	prompt := fmt.Sprintf("请独立编写一幅鹈鹕骑自行车的二维循环动画，场景是%s。画面中要有鹈鹕的大嘴和喉囊、张开的翅膀、双脚与踏板协调运动，以及持续转动的两个车轮。请认真设计构图、颜色与骑行动作。在右下角的 text 元素中写出校验码 %s。只输出完整 SVG 源码，根元素声明 SVG xmlns 并使用 viewBox，允许基本图形、内联 CSS 和 SMIL；动画每周期至少 0.2 秒。禁止 JavaScript、HTML、foreignObject、use、symbol、mask、外部资源、图片及滤镜。不要 Markdown 或解释。", scenes[index], nonce)
	return challenge{kind: PelicanProbeKind, label: "鹈鹕动画校验", prompt: prompt, expected: nonce, maxTokens: 8192, scene: scenes[index], evaluate: func(raw string) (bool, string) {
		svg, err := SafePelicanSVG(raw)
		if err != nil {
			return false, err.Error()
		}
		decoder := xml.NewDecoder(strings.NewReader(svg))
		inText := 0
		var text strings.Builder
		for {
			token, err := decoder.Token()
			if err != nil {
				break
			}
			switch item := token.(type) {
			case xml.StartElement:
				if item.Name.Local == "text" {
					inText++
				}
			case xml.EndElement:
				if item.Name.Local == "text" {
					inText--
				}
			case xml.CharData:
				if inText > 0 {
					text.Write(item)
				}
			}
		}
		if !strings.Contains(text.String(), nonce) {
			return false, "未找到本轮 text 校验码"
		}
		if !strings.Contains(svg, "<animate") && !(strings.Contains(svg, "@keyframes") && strings.Contains(svg, "animation")) {
			return false, "未找到声明的动画"
		}
		return true, "SVG 与校验码通过，画面和动作需人工核对"
	}}, nil
}

// Candy counts are randomized. The player can distinguish shapes, but not
// flavors, so they choose a fixed number of each shape before drawing.
func buildCandyChallenge(random io.Reader) (challenge, error) {
	var counts [6]int
	for i := range counts {
		value, err := randomRange(random, 4, 12)
		if err != nil {
			return challenge{}, err
		}
		counts[i] = value
	}
	nonce, err := randomNonce(random, 6)
	if err != nil {
		return challenge{}, err
	}
	answer := candyMinimum(counts)
	expected := fmt.Sprintf(`{"nonce":%q,"answer":%d}`, nonce, answer)
	prompt := fmt.Sprintf("袋中有苹果、桃子、薄荷三种口味的糖。圆形的数量依次为 %d、%d、%d；星形的数量依次为 %d、%d、%d。你能凭触感区分形状，但无法区分口味。请预先确定各取多少颗圆形和星形糖，要求无论摸到哪些糖，都保证得到一对口味分别为苹果与桃子、且形状不同的糖。最少总共要取几颗？只输出严格 JSON：{\"nonce\":\"%s\",\"answer\":最小总数}，不要解释或额外字段。", counts[0], counts[1], counts[2], counts[3], counts[4], counts[5], nonce)
	return challenge{kind: CandyProbeKind, label: "糖果题", prompt: prompt, expected: expected, maxTokens: 2048, evaluate: func(raw string) (bool, string) {
		var got struct {
			Nonce  string `json:"nonce"`
			Answer int    `json:"answer"`
		}
		if err := decodeStrictJSON(raw, &got); err != nil {
			return false, "回答不是要求的 JSON"
		}
		if got.Nonce != nonce || got.Answer != answer {
			return false, "校验码或最少糖果数量不正确"
		}
		return true, "答案与本轮校验码正确"
	}}, nil
}

func candyMinimum(c [6]int) int {
	rounds, stars := c[0]+c[1]+c[2], c[3]+c[4]+c[5]
	best := rounds + stars
	for r := 0; r <= rounds; r++ {
		for s := 0; s <= stars; s++ {
			// A bad draw contains only one target flavor, or target candies of only
			// one shape. Neutral candies can occur in every such draw.
			bad := r <= c[2] || s <= c[5] || (r <= c[0]+c[2] && s <= c[3]+c[5]) || (r <= c[1]+c[2] && s <= c[4]+c[5])
			if !bad && r+s < best {
				best = r + s
			}
		}
	}
	return best
}

var svgLocalURL = regexp.MustCompile(`(?i)url\(\s*["']?#[a-z0-9_-]+["']?\s*\)`)
var svgComments = regexp.MustCompile(`(?s)/\*.*?\*/`)
var svgElements = map[string]bool{"svg": true, "g": true, "defs": true, "path": true, "rect": true, "circle": true, "ellipse": true, "line": true, "polyline": true, "polygon": true, "text": true, "tspan": true, "textPath": true, "title": true, "desc": true, "style": true, "linearGradient": true, "radialGradient": true, "stop": true, "clipPath": true, "animate": true, "animateTransform": true, "animateMotion": true, "mpath": true}
var svgAnimatedAttributes = map[string]bool{"transform": true, "d": true, "x": true, "y": true, "cx": true, "cy": true, "r": true, "rx": true, "ry": true, "x1": true, "x2": true, "y1": true, "y2": true, "opacity": true, "fill": true, "stroke": true, "stroke-width": true, "stroke-dashoffset": true, "points": true}

// SafePelicanSVG returns only a bounded, self-contained SVG image. It is never
// inserted into the admin DOM; the UI uses an image context as a second fence.
func SafePelicanSVG(raw string) (string, error) {
	if len(raw) == 0 || len(raw) > MaxPelicanSVGBytes {
		return "", errors.New("SVG 为空或超过 96 KiB")
	}
	raw = strings.TrimSpace(raw)
	decoder := xml.NewDecoder(strings.NewReader(raw))
	depth, elements, animations := 0, 0, 0
	styleDepth := 0
	var styleText strings.Builder
	rootSeen, viewBox := false, false
	fail := func() (string, error) { return "", errors.New("SVG 含不支持的结构或外部资源") }
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", errors.New("SVG 不完整或格式错误")
		}
		switch node := token.(type) {
		case xml.StartElement:
			if styleDepth > 0 {
				return fail()
			}
			depth++
			elements++
			if depth > 48 || elements > 2000 || !svgElements[node.Name.Local] || (node.Name.Space != "" && node.Name.Space != "http://www.w3.org/2000/svg") {
				return fail()
			}
			if depth == 1 {
				if rootSeen || node.Name.Local != "svg" || node.Name.Space != "http://www.w3.org/2000/svg" {
					return fail()
				}
				rootSeen = true
			}
			if node.Name.Local == "style" {
				styleDepth = depth
				styleText.Reset()
			}
			if strings.HasPrefix(node.Name.Local, "animate") {
				animations++
				if animations > 64 {
					return fail()
				}
			}
			for _, attr := range node.Attr {
				name, value := strings.ToLower(attr.Name.Local), strings.TrimSpace(attr.Value)
				if attr.Name.Local == "xmlns" || attr.Name.Space == "xmlns" {
					if value != "http://www.w3.org/2000/svg" && value != "http://www.w3.org/1999/xlink" {
						return fail()
					}
					continue
				}
				if strings.HasPrefix(name, "on") || name == "base" || (attr.Name.Space != "" && attr.Name.Space != "http://www.w3.org/1999/xlink") || !safeSVGValue(value) {
					return fail()
				}
				if name == "style" && !safeSVGStyle(value) {
					return fail()
				}
				if name == "filter" || name == "mask" {
					return fail()
				}
				if (name == "href" && (!strings.HasPrefix(value, "#") || len(value) < 2)) || name == "src" {
					return fail()
				}
				if strings.HasPrefix(node.Name.Local, "animate") && name == "attributename" && !svgAnimatedAttributes[value] {
					return fail()
				}
				if strings.HasPrefix(node.Name.Local, "animate") && name == "dur" {
					seconds := strings.TrimSuffix(value, "s")
					factor := 1.0
					if strings.HasSuffix(value, "ms") {
						seconds = strings.TrimSuffix(value, "ms")
						factor = .001
					}
					n, err := strconv.ParseFloat(seconds, 64)
					if err != nil || math.IsNaN(n) || math.IsInf(n, 0) || n*factor < .2 || n*factor > 120 {
						return fail()
					}
				}
				if depth == 1 && attr.Name.Local == "viewBox" {
					parts := strings.Fields(strings.ReplaceAll(value, ",", " "))
					if len(parts) != 4 {
						return fail()
					}
					for i, p := range parts {
						n, err := strconv.ParseFloat(p, 64)
						if err != nil || math.IsNaN(n) || math.IsInf(n, 0) || math.Abs(n) > 4096 || (i > 1 && n <= 0) {
							return fail()
						}
					}
					viewBox = true
				}
			}
		case xml.EndElement:
			if styleDepth == depth {
				if !safeSVGStyle(styleText.String()) {
					return fail()
				}
				styleDepth = 0
			}
			depth--
		case xml.CharData:
			if depth == 0 && strings.TrimSpace(string(node)) != "" {
				return fail()
			}
			if !safeSVGValue(string(node)) {
				return fail()
			}
			if styleDepth > 0 {
				styleText.Write(node)
			}
		case xml.Directive, xml.ProcInst:
			return fail()
		}
	}
	if !rootSeen || depth != 0 || !viewBox {
		return "", errors.New("SVG 缺少根元素或有效 viewBox")
	}
	return raw, nil
}

var svgCSSProperty = regexp.MustCompile(`(?:^|[;{])\s*([-a-zA-Z]+)\s*:\s*([^;{}]+)`)
var svgCSSToken = regexp.MustCompile(`^(?:"[^"]*"|'[^']*'|[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:e[+-]?[0-9]+)?(?:[a-z]+|%)?|[-_a-z][-_a-z0-9]*|!important)`)
var svgCSSTime = regexp.MustCompile(`^([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:e[+-]?[0-9]+)?)(ms|s)$`)
var svgCSSTimingFunction = regexp.MustCompile(`(?:cubic-bezier|steps|linear)\([^()]*\)`)
var svgCSSProperties = map[string]bool{
	"fill": true, "fill-opacity": true, "fill-rule": true, "stroke": true, "stroke-width": true, "stroke-opacity": true, "stroke-linecap": true, "stroke-linejoin": true, "stroke-dasharray": true, "stroke-dashoffset": true,
	"opacity": true, "color": true, "stop-color": true, "stop-opacity": true, "clip-path": true, "clip-rule": true,
	"transform": true, "transform-origin": true, "transform-box": true, "translate": true, "rotate": true, "scale": true,
	"font-family": true, "font-size": true, "font-weight": true, "font-style": true, "text-anchor": true, "dominant-baseline": true, "letter-spacing": true, "paint-order": true,
	"display": true, "visibility": true, "overflow": true, "width": true, "height": true,
	"animation": true, "animation-name": true, "animation-duration": true, "animation-delay": true, "animation-timing-function": true, "animation-iteration-count": true, "animation-direction": true, "animation-fill-mode": true, "animation-play-state": true,
}

func safeSVGStyle(value string) bool {
	value = strings.ToLower(svgComments.ReplaceAllString(value, " "))
	if len(value) > 16384 || !safeSVGValue(value) {
		return false
	}
	animations := 0
	for _, property := range svgCSSProperty.FindAllStringSubmatch(value, -1) {
		name, definition := property[1], strings.TrimSpace(property[2])
		if !svgCSSProperties[name] {
			return false
		}
		if name == "animation-name" && (len(strings.Split(definition, ",")) > 4 || strings.Contains(definition, "(")) {
			return false
		}
		if name == "animation" || strings.HasSuffix(name, "-animation") || strings.HasSuffix(name, "animation-duration") {
			definition = svgCSSTimingFunction.ReplaceAllString(definition, " ")
			if strings.ContainsAny(definition, "()") || len(strings.Split(definition, ",")) > 4 {
				return false
			}
			for _, part := range strings.Split(definition, ",") {
				if strings.TrimSpace(part) == "none" {
					continue
				}
				animations++
				if animations > 64 || !safeSVGAnimationDuration(part) {
					return false
				}
			}
		}
	}
	return true
}

func safeSVGAnimationDuration(value string) bool {
	// Tokenize before looking for a time: an identifier/string may contain
	// digits, and CSS does not require spaces around string or comment tokens.
	for value = strings.TrimSpace(value); value != ""; value = strings.TrimSpace(value) {
		token := svgCSSToken.FindString(value)
		if token == "" {
			return false
		}
		if match := svgCSSTime.FindStringSubmatch(token); len(match) != 0 {
			seconds, err := strconv.ParseFloat(match[1], 64)
			if match[2] == "ms" {
				seconds *= .001
			}
			return err == nil && seconds >= .2 && seconds <= 120
		}
		value = value[len(token):]
	}
	return false
}

func safeSVGValue(value string) bool {
	value = strings.ToLower(svgComments.ReplaceAllString(value, ""))
	value = svgLocalURL.ReplaceAllString(value, "")
	for _, bad := range []string{"\\", "url(", "@import", "@font-face", "javascript:", "http:", "https:", "data:", "expression(", "-moz-binding"} {
		if strings.Contains(value, bad) {
			return false
		}
	}
	return true
}
