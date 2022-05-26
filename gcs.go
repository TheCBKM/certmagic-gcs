package certmagic_gcs

import (
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"fmt"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"io/fs"
	"io/ioutil"
)

type GCS struct {
	logger     *zap.Logger
	BucketName string `json:"bucket"`
	Bucket     *storage.BucketHandle
}

func init() {
	caddy.RegisterModule(GCS{})
}

func (gcs *GCS) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		var value string

		key := d.Val()

		if !d.Args(&value) {
			continue
		}

		switch key {
		case "bucket":
			gcs.BucketName = value
		}
	}

	return nil
}

func (gcs *GCS) Provision(ctx caddy.Context) error {
	gcs.logger = ctx.Logger(gcs)

	// GCS Client
	client, err := storage.NewClient(ctx)
	bucket := client.Bucket(gcs.BucketName)

	if err != nil {
		return err
	} else {
		gcs.Bucket = bucket
	}

	return nil
}

func (GCS) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "caddy.storage.gcs",
		New: func() caddy.Module {
			return new(GCS)
		},
	}
}

func (gcs *GCS) CertMagicStorage() (certmagic.Storage, error) {
	return gcs, nil
}

func (gcs *GCS) Lock(ctx context.Context, key string) error {
	return nil
}

func (gcs *GCS) Unlock(ctx context.Context, key string) error {
	return nil
}

func (gcs *GCS) Store(ctx context.Context, key string, value []byte) error {
	w := gcs.Bucket.Object(key).NewWriter(ctx)

	//encrypted, err := s.aead.Encrypt(value, []byte(key))
	//if err != nil {
	//	return fmt.Errorf("encrypting object %s: %w", key, err)
	//}
	if _, err := w.Write(value); err != nil {
		return fmt.Errorf("writing object %s: %w", key, err)
	}
	return w.Close()
}

func (gcs GCS) Load(ctx context.Context, key string) ([]byte, error) {

	rc, err := gcs.Bucket.Object(key).NewReader(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return nil, fs.ErrNotExist
	}
	if err != nil {
		return nil, fmt.Errorf("loading object %s: %w", key, err)
	}
	defer rc.Close()

	encrypted, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("reading object %s: %w", key, err)
	}

	//decrypted, err := s.aead.Decrypt(encrypted, []byte(key))
	//if err != nil {
	//	return nil, fmt.Errorf("decrypting object %s: %w", key, err)
	//}
	return encrypted, nil
}

func (gcs GCS) Delete(ctx context.Context, key string) error {

	err := gcs.Bucket.Object(key).Delete(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return fs.ErrNotExist
	}
	if err != nil {
		return fmt.Errorf("deleting object %s: %w", key, err)
	}
	return nil
}

func (gcs GCS) Exists(ctx context.Context, key string) bool {

	_, err := gcs.Bucket.Object(key).Attrs(ctx)
	return err != storage.ErrObjectNotExist
}

func (gcs GCS) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	query := &storage.Query{Prefix: prefix}
	if !recursive {
		query.Delimiter = "/"
	}
	var names []string
	it := gcs.Bucket.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("listing objects: %w", err)
		}
		if attrs.Name != "" {
			names = append(names, attrs.Name)
		}
	}
	return names, nil
}

func (gcs GCS) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {

	var keyInfo certmagic.KeyInfo
	attr, err := gcs.Bucket.Object(key).Attrs(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return keyInfo, fs.ErrNotExist
	}
	if err != nil {
		return keyInfo, fmt.Errorf("loading attributes for %s: %w", key, err)
	}
	keyInfo.Key = key
	keyInfo.Modified = attr.Updated
	keyInfo.Size = attr.Size
	keyInfo.IsTerminal = true
	return keyInfo, nil
}
