/*
Copyright 2021 The Kruise Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package imageruntime

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	daemonutil "github.com/openkruise/kruise/pkg/daemon/util"
	"github.com/pkg/errors"
	"golang.org/x/net/http/httpproxy"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
	"k8s.io/klog/v2"
	internalapi "k8s.io/cri-api/pkg/apis"
	kubeletutil "k8s.io/kubernetes/pkg/kubelet/util"
	criremote "k8s.io/kubernetes/pkg/kubelet/cri/remote"
)

// NewCriImageService returns cri-type ImageService
func NewCriImageService(
	runtimeRemoteURI string,
	accountManager daemonutil.ImagePullAccountManager,
) (ImageService, error) {
	timeout := 10 * time.Second
	maxMsgSize := 1024 * 1024 * 16

	addr, dialer, err := kubeletutil.GetAddressAndDialer(runtimeRemoteURI)
	if err != nil {
		return nil, err
	}

	gopts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithContextDialer(dialer),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, gopts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial %q", addr)
	}

	return &criImageService{
		accountManager: accountManager,
		criImageClient: runtimeapi.NewImageServiceClient(conn),
	}, nil
}

type criImageService struct {
	accountManager daemonutil.ImagePullAccountManager
	criImageClient runtimeapi.ImageServiceClient
}

// PullImage implements ImageService.PullImage.
func (d *criImageService) PullImage(ctx context.Context, imageName, tag string, pullSecrets []v1.Secret) (reader ImagePullStatusReader, err error) {
	if tag == "" {
		tag = defaultTag
	}

	registry := daemonutil.ParseRegistry(imageName)
	fullName := fmt.Sprintf("%s:%s", imageName, tag)

	if len(pullSecrets) > 0 {
		var authInfos []daemonutil.AuthInfo
		authInfos, err = convertToRegistryAuths(pullSecrets, registry)
		if err == nil {
			var pullErrs []error
			for _, authInfo := range authInfos {
				var pullErr error
				klog.V(5).Infof("Pull image %v:%v with user %v", imageName, tag, authInfo.Username)
				ioReader, pullErr = d.client.ImagePull(ctx, fullName, dockertypes.ImagePullOptions{RegistryAuth: authInfo.EncodeToString()})
				if pullErr == nil {
					return newImagePullStatusReader(ioReader), nil
				}
				d.handleRuntimeError(pullErr)
				klog.Warningf("Failed to pull image %v:%v with user %v, err %v", imageName, tag, authInfo.Username, pullErr)
				pullErrs = append(pullErrs, pullErr)
			}
			if len(pullErrs) > 0 {
				err = utilerrors.NewAggregate(pullErrs)
			}
		}
	}

	// Try the default secret
	if d.accountManager != nil {
		var authInfo *daemonutil.AuthInfo
		var defaultErr error
		authInfo, defaultErr = d.accountManager.GetAccountInfo(registry)
		if defaultErr != nil {
			klog.Warningf("Failed to get account for registry %v, err %v", registry, defaultErr)
			// When the default account acquisition fails, try to pull anonymously
		} else if authInfo != nil {
			klog.V(5).Infof("Pull image %v:%v with user %v", imageName, tag, authInfo.Username)
			ioReader, err = d.client.ImagePull(ctx, fullName, dockertypes.ImagePullOptions{RegistryAuth: authInfo.EncodeToString()})
			if err == nil {
				return newImagePullStatusReader(ioReader), nil
			}
			d.handleRuntimeError(err)
			klog.Warningf("Failed to pull image %v:%v, err %v", imageName, tag, err)
		}
	}

	if err != nil {
		return nil, err
	}

	// Anonymous pull
	klog.V(5).Infof("Pull image %v:%v anonymous", imageName, tag)
	ioReader, err = d.client.ImagePull(ctx, fullName, dockertypes.ImagePullOptions{})
	if err != nil {
		d.handleRuntimeError(err)
		return nil, err
	}
	

	ctx, cancel := getContextWithCancel()
	defer cancel()

	resp, err := r.imageClient.PullImage(ctx, &runtimeapi.PullImageRequest{
		Image:         image,
		Auth:          auth,
		SandboxConfig: podSandboxConfig,
	})
	if err != nil {
		klog.Errorf("PullImage %q from image service failed: %v", image.Image, err)
		return "", err
	}

	if resp.ImageRef == "" {
		errorMessage := fmt.Sprintf("imageRef of image %q is not set", image.Image)
		klog.Errorf("PullImage failed: %s", errorMessage)
		return "", errors.New(errorMessage)
	}

	return resp.ImageRef, nil

}

// ListImages implements ImageService.ListImages.
func (d *criImageService) ListImages(ctx context.Context) ([]ImageInfo, error) {
	resp, err := d.criImageClient.ListImages(ctx, &runtimeapi.ListImagesRequest{})
	if err != nil {
		return nil, err
	}

	collection := make([]ImageInfo, 0, len(resp.Images))
	for _, info := range resp.Images {
		collection = append(collection, ImageInfo{
			ID:          info.Id,
			RepoTags:    info.RepoTags,
			RepoDigests: info.RepoDigests,
			Size:        int64(info.Size_),
		})
	}
	return collection, nil
}
