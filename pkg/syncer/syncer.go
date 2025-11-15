package syncer

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"strings"

	v1 "agones.dev/agones/pkg/apis/agones/v1"
	"github.com/Octops/agones-event-broadcaster/pkg/events"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	pb "k8s.io/cri-api/pkg/apis/runtime/v1"

	"github.com/haabiz-game/image-syncer/pkg/runtime/log"
)

type ImageServiceClient interface {
	ImageStatus(ctx context.Context, request *pb.ImageStatusRequest) (*pb.ImageStatusResponse, error)
	PullImage(ctx context.Context, request *pb.PullImageRequest) (*pb.PullImageResponse, error)
}

// FleetImageSyncer implements the Broker interface used by the Agones Event Broadcaster to notify events
type FleetImageSyncer struct {
	imageClient ImageServiceClient
	config      *rest.Config
}

func NewFleetImageSyncer(client ImageServiceClient, config *rest.Config) *FleetImageSyncer {
	return &FleetImageSyncer{client, config}
}

func (f *FleetImageSyncer) BuildEnvelope(event events.Event) (*events.Envelope, error) {
	envelope := &events.Envelope{}
	envelope.AddHeader("event_type", event.EventType().String())
	envelope.Message = event.(events.Message)

	return envelope, nil
}

func (f *FleetImageSyncer) SendMessage(envelope *events.Envelope) error {
	message := envelope.Message.(events.Message).Content()
	eventType := envelope.Header.Headers["event_type"]

	fleet, err := f.Unwrap(message)
	if err != nil {
		return errors.Wrap(err, "failed to process event")
	}

	switch eventType {
	case "fleet.events.added":
		fallthrough
	case "fleet.events.updated":
		return f.HandleAddedUpdated(fleet)
	case "fleet.events.deleted":
		//TODO: Consider a flag to decide if the image must be removed when a fleet is deleted
		//It may cause a race condition with running gameservers that are still in Terminating state
		log.Logger().Infof("fleet %s deleted", fleet.Name)
	}

	return nil
}

func (f *FleetImageSyncer) HandleAddedUpdated(fleet *v1.Fleet) error {
	image := fleet.Spec.Template.Spec.Template.Spec.Containers[0].Image

	fields := logrus.Fields{
		"fleet":           fleet.GetName(),
		"image":           image,
		"imagePullSecret": fleet.Spec.Template.Spec.Template.Spec.ImagePullSecrets,
	}

	if ok, err := f.CheckImageStatus(image); err != nil {
		return errors.Wrap(err, "failed to check image status")
	} else if ok {
		log.Logger().WithFields(fields).Info("image already present")

		return nil
	}
	var auth *pb.AuthConfig
	if len(fleet.Spec.Template.Spec.Template.Spec.ImagePullSecrets) > 0 {
		var err error
		auth, err = f.getPullSecret(fleet.GetNamespace(), fleet.Spec.Template.Spec.Template.Spec.ImagePullSecrets[0].Name)
		if err != nil {
			log.Logger().WithError(err).WithFields(fields).Warn("failed to read imagePullSecret; proceeding without auth")
		}
	}
	fields["auth"] = auth

	ref, err := f.PullImage(image, auth)
	if err != nil {
		log.Logger().WithError(err).WithFields(fields).Error("failed to pull image")
		return nil
	}

	log.Logger().WithFields(fields).WithField("ref", ref).Info("fleet synced")

	return nil
}

func (f *FleetImageSyncer) Unwrap(message interface{}) (*v1.Fleet, error) {
	if fleet, ok := message.(*v1.Fleet); ok {
		return fleet, nil
	} else if fleet, ok := reflect.ValueOf(message).Field(1).Interface().(*v1.Fleet); ok {
		return fleet, nil
	}

	return nil, errors.New("message content is not a v1.Fleet")
}

func (f *FleetImageSyncer) CheckImageStatus(image string) (bool, error) {
	statusRequest := createImageStatusRequest(image)

	status, err := f.imageClient.ImageStatus(context.Background(), statusRequest)
	if err != nil {
		return false, errors.Wrap(err, "failed to get image status")
	}

	if status.Image != nil && len(status.Image.Id) > 0 {
		return true, nil
	}

	//Image is not present
	return false, nil
}

func (f *FleetImageSyncer) PullImage(image string, auth *pb.AuthConfig) (string, error) {
	request := createPullImageRequest(image, auth)

	resp, err := f.imageClient.PullImage(context.Background(), request)
	if err != nil {
		return "", errors.Wrap(err, "failed to pull image")
	}

	return resp.GetImageRef(), nil
}

func (f *FleetImageSyncer) getPullSecret(namespace, name string) (*pb.AuthConfig, error) {
	ctx := context.Background()
	k, err := kubernetes.NewForConfig(f.config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create kubernetes client")
	}
	sec, err := k.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get imagePullSecret")
	}

	switch sec.Type {
	case corev1.SecretTypeDockerConfigJson:
		b, ok := sec.Data[corev1.DockerConfigJsonKey]
		if !ok {
			return nil, errors.New(".dockerconfigjson key missing in secret")
		}
		return parseDockerConfigJSON(b)
	default:
		return nil, errors.Errorf("unsupported imagePullSecret type: %s (only kubernetes.io/dockerconfigjson supported)", sec.Type)
	}
}

type dockerAuthEntry struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Auth     string `json:"auth"`
}

type dockerConfigJSON struct {
	Auths map[string]dockerAuthEntry `json:"auths"`
}

func parseDockerConfigJSON(b []byte) (*pb.AuthConfig, error) {
	var cfg dockerConfigJSON
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal dockerconfigjson")
	}
	if len(cfg.Auths) == 0 {
		return nil, errors.New("dockerconfigjson has no auths")
	}

	// Prefer GHCR if present, else first entry
	var server string
	var ent dockerAuthEntry
	if e, ok := pickAuth(cfg.Auths, "ghcr.io"); ok {
		server, ent = e.server, e.entry
	} else {
		for k, v := range cfg.Auths {
			server = k
			ent = v
			break
		}
	}
	return toCRIAuth(ent, server), nil
}

type authPick struct {
	server string
	entry  dockerAuthEntry
}

func pickAuth(m map[string]dockerAuthEntry, hint string) (authPick, bool) {
	// exact match
	for k, v := range m {
		if normalizeRegistry(k) == normalizeRegistry(hint) {
			return authPick{server: k, entry: v}, true
		}
	}
	// contains match (handles https://ghcr.io/)
	for k, v := range m {
		if strings.Contains(normalizeRegistry(k), normalizeRegistry(hint)) {
			return authPick{server: k, entry: v}, true
		}
	}
	return authPick{}, false
}

func toCRIAuth(ent dockerAuthEntry, server string) *pb.AuthConfig {
	username := ent.Username
	password := ent.Password
	if (username == "" || password == "") && ent.Auth != "" {
		if dec, err := base64.StdEncoding.DecodeString(ent.Auth); err == nil {
			parts := strings.SplitN(string(dec), ":", 2)
			if len(parts) == 2 {
				username, password = parts[0], parts[1]
			}
		}
	}
	return &pb.AuthConfig{
		Username:      username,
		Password:      password,
		Auth:          ent.Auth,
		ServerAddress: normalizeRegistry(server),
	}
}

func normalizeRegistry(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "https://")
	s = strings.TrimPrefix(s, "http://")
	s = strings.TrimSuffix(s, "/")
	return s
}

func createPullImageRequest(image string, auth *pb.AuthConfig) *pb.PullImageRequest {
	return &pb.PullImageRequest{
		Image: &pb.ImageSpec{
			Image: image,
		},
		Auth: auth,
	}
}

func createImageStatusRequest(image string) *pb.ImageStatusRequest {
	return &pb.ImageStatusRequest{
		Image: &pb.ImageSpec{
			Image: image,
		},
	}
}
