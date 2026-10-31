// Command localbrain is a minimal local UHP harness with a scripted model. It
// listens on loopback and accepts the harness access token from
// UHP_HARNESS_TOKEN; the platform (or any uhpclient) drives it at
// http://<addr>/v1/uhp exactly as it drives BMA's UHP gateway.
//
//	UHP_HARNESS_TOKEN=... go run ./uhpharness/example/localbrain -addr 127.0.0.1:8765
package main

import (
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/beeos-ai/bma-sdks/go/uhp"
	"github.com/beeos-ai/bma-sdks/go/uhpharness"
)

func newServer(harnessID, token, root string, tick time.Duration) (*uhpharness.Server, error) {
	return uhpharness.New(uhpharness.Config{
		Harness: uhp.Harness{Id: harnessID, Name: "Local brain (scripted)", Base: "beeos-local-brain",
			DefaultModel: ptr(defaultModel)},
		Handler:            &brain{Root: root, Tick: tick},
		Auth:               uhpharness.BearerToken(token, "platform"),
		ImplementationName: "localbrain-example", ImplementationVersion: "0.1.0",
	})
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8765", "listen address (keep it on loopback)")
	harnessID := flag.String("harness-id", "chrn_localbrain", "harness id assigned at registration")
	root := flag.String("root", os.TempDir(), "the only directory tree a turn may use as cwd")
	flag.Parse()
	token := os.Getenv("UHP_HARNESS_TOKEN")
	if token == "" {
		log.Fatal("UHP_HARNESS_TOKEN is required")
	}
	server, err := newServer(*harnessID, token, *root, 100*time.Millisecond)
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()
	log.Printf("UHP harness %s at http://%s%s", *harnessID, *addr, uhpharness.DefaultRoot)
	httpServer := &http.Server{Addr: *addr, Handler: server.Handler(), ReadHeaderTimeout: 10 * time.Second}
	if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
