package api

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
	"os"
	"path/filepath"
	"time"
)

// StoreSnaps returns a channel of potentially-transformed CatTracks and a channel of errors.
// If the incoming cattracks are not snaps, they are forwarded to the output channel.
// Incoming cattrack Snaps are mutated/transformed -- stripping Base64 data, adding storage URLs --
// and likewise forwarded to the output. Output is unbuffered and blocking, requires a consumer.
// Remember: Snaps for which the handler errors ARE ALSO forwarded to the output channel.
// This ensures that subsequent track handlers do not miss _any_ tracks because of any Snap logic issues.
// Errors are forwarded to the error channel.
// The handler is idempotent and can be run multiple times on the same input.
//
// Cat Snaps are originally uploaded by the client encoded in base64 in a properties attribute 'imgB64'.
// This handler attempts to decode the data, store it locally as a .jpg, and then upload it to S3.
// If decoding fails, the original track is forwarded to the output channel unmodified.
// If upload is successful, the track is modified in-place to include the S3 URL in an attribute 'imgS3',
// and the original `imgB64` attribute is removed.
// If upload fails, the original track is forwarded to the output channel unmodified.
// If the cat handler finds that the snap already exists in the cat state, it is not uploaded again, nor transformed.
func (c *Cat) StoreSnaps(ctx context.Context, in <-chan cattrack.CatTrack) (out chan cattrack.CatTrack, errs chan error) {
	c.getOrInitState()

	c.State.Waiting.Add(1)
	defer c.State.Waiting.Done()

	out = make(chan cattrack.CatTrack)
	errs = make(chan error)
	go func() {
		defer close(out)
		defer close(errs)

		for track := range in {
			track := track
			if !track.IsSnap() {
				out <- track
				continue
			}
			handled, err := c.handleSnap(track)
			if err != nil {
				errs <- err
			}
			out <- handled
		}
	}()

	return out, errs
}

// handleSnap is a private method that handles a single CatTrack Snap.
// It is careful to modify the track in-place, as it is a pointer,
// _iff any transformation operations are successful_.
// If any transformation operations fail, the original track remains unmodified.
func (c *Cat) handleSnap(ct cattrack.CatTrack) (cattrack.CatTrack, error) {

	if err := c.State.ValidateSnapLocalStore(ct); err == nil {
		c.logger.Warn("Snap already exists", "cat", c.CatID, "track", ct.StringPretty())
		return ct, nil
	}

	if err := ct.ValidateSnap(); err != nil {
		return ct, err
	}

	scp, err := c.importCatSnap(ct)
	if err != nil {
		c.logger.Error("Failed to import snap", "error", err)
		return scp, err
	}

	// All errors nil, we can now modify the original track:
	return scp, nil
}

func (c *Cat) importCatSnap(ct cattrack.CatTrack) (imported cattrack.CatTrack, err error) {
	c.logger.Info("📷 Importing snap", "track", ct.StringPretty())
	imported = ct
	if ct.HasRawB64Image() {
		jpegBytes, err := common.DecodeB64ToJPGBytes(ct.Properties.MustString("imgB64"))
		if err != nil {
			return ct, err
		}

		// Attempt AWS S3 upload.
		if params.AWS_BUCKETNAME == "" {
			imported.Properties["imgS3_UPLOAD_SKIPPED"] = time.Now()
			c.logger.Warn("AWS_BUCKETNAME not set, skipping S3 upload", "track", ct.StringPretty())

		} else {
			err = c.storeImageS3(ct.MustS3Key(), jpegBytes)
			if err != nil {
				imported.Properties["imgS3_UPLOAD_FAILED"] = time.Now()
			}

			imported.Properties["imgS3"] = fmt.Sprintf("%s/%s",
				params.AWS_BUCKETNAME, ct.MustS3Key())
		}

		err = c.State.StoreSnapLocally(imported, jpegBytes)
		if err != nil {
			c.logger.Error("Failed to store snap", "error", err)
			return imported, err
		}

		delete(imported.Properties, "imgB64")
		return imported, nil
	}

	if !ct.HasS3URL() {
		panic("impossible")
	}

	// If the snap is already in S3, we don't need to do anything.
	if err := c.State.ValidateSnapLocalStore(imported); err == nil {
		return imported, nil
	}

	// Store KV before trying download.
	if err := c.State.StoreSnapLocally(imported, nil); err != nil {
		return imported, err
	}

	if params.AWS_BUCKETNAME == "" {
		c.logger.Warn("AWS_BUCKETNAME not set, skipping S3 download", "track", ct.StringPretty())
		return imported, nil
	}

	// If the snap is in S3 but not in the local state, we need to download it.
	// WARNING: Waiting on the download slows things down a lot.
	// For long-lived cats only.
	// But if we DON'T await it, the downloads can get killed and result in corrupted files.
	// FIXME: Write a 'RecoverSnaps' operation that a daemon can use to fix up cats and their snaps.
	c.State.Waiting.Add(1)
	go func() {
		defer c.State.Waiting.Done()
		target := c.State.SnapImagePath(ct)
		if err := os.MkdirAll(filepath.Dir(target), 0770); err != nil {
			return
		}
		f, err := os.Create(target)
		if err != nil {
			c.logger.Error("Failed to create snap file (downloading)", "error", err)
			return
		}
		defer f.Close()
		start := time.Now()
		err = c.downloadImageS3(f, params.AWS_BUCKETNAME, ct.MustS3Key())
		if err != nil {
			c.logger.Error("Failed to download snap", "error", err)
		}
		if err := f.Sync(); err != nil {
			return
		}
		c.logger.Info("↧ Downloaded snap", "to", target,
			"elapsed", time.Since(start).Round(time.Millisecond))
	}()

	return imported, nil
}

func (c *Cat) storeImageS3(key string, jpegBytes []byte) (err error) {

	// S3

	// All clients require a Session. The Session provides the client with
	// shared configuration such as region, endpoint, and credentials. A
	// Session should be shared where possible to take advantage of
	// configuration and credential caching. See the session package for
	// more information.
	sess := session.Must(session.NewSession())

	// Create a new instance of the service's client with a Session.
	// Optional aws.Config values can also be provided as variadic arguments
	// to the New function. This option allows you to provide service
	// specific configuration.
	svc := s3.New(sess)

	// Create a context with a timeout that will abort the upload if it takes
	// more than the passed in timeout.
	ctx := context.Background()
	var cancelFn func()
	timeout := time.Second * 10
	if timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, timeout)
	}

	// Ensure the context is canceled to prevent leaking.
	// See context package for more information, https://golang.org/pkg/context/
	defer cancelFn()

	// Uploads the object to S3. The Context will interrupt the request if the
	// timeout expires.
	_, err = svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(params.AWS_BUCKETNAME),
		Key:           aws.String(key),
		Body:          bytes.NewReader(jpegBytes),
		ContentType:   aws.String("image/jpeg"),
		ContentLength: aws.Int64(int64(len(jpegBytes))),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			c.logger.Error("AWS S3 upload canceled due to timeout", "error", err)
		} else {
			c.logger.Error("Failed to upload object", "error", err)
		}
		return err
	}

	c.logger.Info("Uploaded image to AWS S3", "bucket", params.AWS_BUCKETNAME, "key", key)
	return nil
}

// downloadImageS3 downloads an image from S3 and writes it to the provided writer.
// The AWS library uses environment variables to configure itself.
func (c *Cat) downloadImageS3(wr io.WriterAt, bucket, key string) error {

	// The session the S3 Downloader will use
	sess := session.Must(session.NewSession())

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(sess)

	// Write the contents of S3 Object to the file
	c.logger.Info("Downloading image from S3...", "bucket", bucket, "key", key)
	_, err := downloader.Download(wr, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to download S3 file, %v", err)
	}
	return nil
}
