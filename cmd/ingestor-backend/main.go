package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/kubetrace/ingestor-backend/internal/ingestor"
)

func main() {
	cfg := ingestor.ParseConfig()
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	cfg.Log()

	service := ingestor.New(cfg)
	defer service.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := service.InitDatabaseSchema(); err != nil {
		log.Fatalf("database schema init failed: %v", err)
	}

	go service.ConsumeLoop(ctx)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down ingestor...")
	cancel()
	log.Println("Ingestor stopped.")
}
