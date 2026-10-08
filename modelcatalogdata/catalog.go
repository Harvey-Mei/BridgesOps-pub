package modelcatalogdata

import _ "embed"

// JSON is the single model-family catalog consumed by both the Go backend and
// the TypeScript frontend.
//
//go:embed catalog.json
var JSON []byte
