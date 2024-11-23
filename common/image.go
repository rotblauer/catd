package common

import (
	"bytes"
	"encoding/base64"
	"image/jpeg"
)

func DecodeB64ToJPGBytes(b64 string) ([]byte, error) {
	unbased, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, err
	}

	r := bytes.NewReader(unbased)
	im, err := jpeg.Decode(r)
	if err != nil {
		return nil, err
	}

	b := []byte{}
	buf := bytes.NewBuffer(b)
	err = jpeg.Encode(buf, im, &jpeg.Options{Quality: 100})
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
