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
	return &FleetImageSyncer{imageClient: client, config: config}
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
        a, err := f.authFromPullSecrets(
            context.Background(),
            fleet.GetNamespace(),
            fleet.Spec.Template.Spec.Template.Spec.ImagePullSecrets,
            image,
        )
        if err != nil {
            log.Logger().WithError(err).WithFields(fields).Warn("failed to read imagePullSecrets; proceeding without auth")
        } else {
            auth = a
        }
    }

	ref, err := f.PullImage(image, auth)
	if err != nil {
		return errors.Wrap(err, "failed to pull image")
	}

	log.Logger().WithFields(fields).WithField("ref", ref).Info("fleet synced")

	return nil
}

// authFromPullSecrets inspects the provided imagePullSecrets and returns a CRI AuthConfig
// matching the registry used by the provided image. If no suitable secret is found, it returns nil.
func (f *FleetImageSyncer) authFromPullSecrets(ctx context.Context, namespace string, pullSecrets []corev1.LocalObjectReference, image string) (*pb.AuthConfig, error) {
    if f.config == nil {
        return nil, errors.New("kube rest.Config is nil")
    }

    clientset, err := kubernetes.NewForConfig(f.config)
    if err != nil {
        return nil, errors.Wrap(err, "failed to create kubernetes clientset")
    }

    // Determine registry key from image
    registryHint := registryFromImage(image)

    for _, ps := range pullSecrets {
        sec, err := clientset.CoreV1().Secrets(namespace).Get(ctx, ps.Name, metav1.GetOptions{})
        if err != nil {
            // Try next secret; log at debug level via trace
            log.Logger().WithError(err).WithField("secret", ps.Name).Debug("failed to get imagePullSecret")
            continue
        }

        // Support both dockerconfigjson and dockercfg
        switch sec.Type {
        case corev1.SecretTypeDockerConfigJson:
            if b, ok := sec.Data[corev1.DockerConfigJsonKey]; ok {
                if ac := authFromDockerConfigJSON(b, registryHint); ac != nil {
                    return ac, nil
                }
            }
        case corev1.SecretTypeDockercfg:
            if b, ok := sec.Data[corev1.DockerConfigKey]; ok {
                if ac := authFromDockercfg(b, registryHint); ac != nil {
                    return ac, nil
                }
            }
        default:
            // ignore unsupported types
            continue
        }
    }

    return nil, nil
}

type dockerAuthEntry struct {
    Username      string `json:"username"`
    Password      string `json:"password"`
    Auth          string `json:"auth"`
    IdentityToken string `json:"identitytoken"`
    RegistryToken string `json:"registrytoken"`
}

type dockerConfigJSON struct {
    Auths map[string]dockerAuthEntry `json:"auths"`
}

func authFromDockerConfigJSON(b []byte, registryHint string) *pb.AuthConfig {
    var cfg dockerConfigJSON
    if err := json.Unmarshal(b, &cfg); err != nil {
        return nil
    }
    key, ent := findBestAuthMatch(cfg.Auths, registryHint)
    if ent == nil {
        return nil
    }
    return toCRIAuth(ent, key)
}

func authFromDockercfg(b []byte, registryHint string) *pb.AuthConfig {
    var cfg map[string]dockerAuthEntry
    if err := json.Unmarshal(b, &cfg); err != nil {
        return nil
    }
    key, ent := findBestAuthMatch(cfg, registryHint)
    if ent == nil {
        return nil
    }
    return toCRIAuth(ent, key)
}

func toCRIAuth(ent *dockerAuthEntry, server string) *pb.AuthConfig {
    username := ent.Username
    password := ent.Password
    if (username == "" || password == "") && ent.Auth != "" {
        if dec, err := base64.StdEncoding.DecodeString(ent.Auth); err == nil {
            parts := strings.SplitN(string(dec), ":", 2)
            if len(parts) == 2 {
                username = parts[0]
                password = parts[1]
            }
        }
    }

    return &pb.AuthConfig{
        Username:      username,
        Password:      password,
        Auth:          ent.Auth,
        ServerAddress: server,
        IdentityToken: ent.IdentityToken,
        RegistryToken: ent.RegistryToken,
    }
}

// findBestAuthMatch picks the best matching registry entry based on the image registry hint.
func findBestAuthMatch(m map[string]dockerAuthEntry, registryHint string) (string, *dockerAuthEntry) {
    if len(m) == 0 {
        return "", nil
    }

    // exact match
    if e, ok := m[registryHint]; ok {
        return registryHint, &e
    }

    // common docker.io aliases
    dockerAliases := []string{
        "https://index.docker.io/v1/",
        "index.docker.io",
        "registry-1.docker.io",
        "docker.io",
    }
    if isDockerHub(registryHint) {
        for _, a := range dockerAliases {
            if e, ok := m[a]; ok {
                return a, &e
            }
        }
    }

    // fallback: single entry or any entry containing the hint
    if len(m) == 1 {
        for k, v := range m {
            return k, &v
        }
    }
    for k, v := range m {
        if strings.Contains(k, registryHint) {
            vv := v
            return k, &vv
        }
    }
    return "", nil
}

func registryFromImage(image string) string {
    // default docker hub
    reg := "docker.io"
    // If image includes a registry (has a domain or :port in first segment), use it
    first := image
    if i := strings.Index(image, "/"); i != -1 {
        first = image[:i]
    }
    if strings.Contains(first, ".") || strings.Contains(first, ":") || first == "localhost" {
        reg = first
    }
    return reg
}

func isDockerHub(reg string) bool {
    reg = strings.TrimSuffix(reg, "/")
    return reg == "docker.io" || reg == "index.docker.io" || reg == "registry-1.docker.io" || reg == "https://index.docker.io/v1"
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
