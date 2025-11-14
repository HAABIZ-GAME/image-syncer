package cmd

import (
	"context"
	"os"
	"time"

	"github.com/pkg/errors"
	"k8s.io/client-go/rest"

	"github.com/haabiz-game/image-syncer/pkg/clients"
	"github.com/haabiz-game/image-syncer/pkg/runtime/log"
	"github.com/haabiz-game/image-syncer/pkg/syncer"
	"github.com/haabiz-game/image-syncer/pkg/transport"
	"github.com/haabiz-game/image-syncer/pkg/watcher"
)

func Execute(ctx context.Context, config *rest.Config, duration time.Duration, port int, metricsBindAddress string) error {
	target := os.Getenv("CONN_TARGET")

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := transport.NewConn(ctx, target)
	if err != nil {
		return errors.Wrapf(err, "failed to create connection to: %s", target)
	}
	defer conn.Close()
	log.Logger().Infof("connected to %s", target)

	imageServiceClient := clients.NewImageServiceClient(conn)
	imageSyncer := syncer.NewFleetImageSyncer(imageServiceClient)
	watcherConfig := &watcher.Config{
		ClientConfig:   config,
		Duration:       duration,
		Port:           port,
		MetricsAddress: metricsBindAddress,
	}

	fleetWatcher, err := watcher.NewFleetWatcher(watcherConfig, imageSyncer)
	if err != nil {
		return errors.Wrap(err, "failed to create watcher")
	}

	if err := fleetWatcher.Start(ctx); err != nil {
		return errors.Wrap(err, "failed to start fleet watcher")
	}

	return nil
}
