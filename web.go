// Package queuemaxxing embeds the demo web UI so the server ships as a single static binary
// with no runtime file dependencies, which is what allows the scratch container image.
package queuemaxxing

import (
	"embed"
	"io/fs"
)

//go:embed web
var webFiles embed.FS

func WebFS() fs.FS {
	sub, err := fs.Sub(webFiles, "web")
	if err != nil {
		panic(err) // the embed directive above guarantees the subtree exists
	}
	return sub
}
