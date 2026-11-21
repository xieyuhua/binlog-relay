package fullsync

import (
	"encoding/base64"
	"unicode/utf8"
)

func base64Encode(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
func isUTF8(b []byte) bool         { return utf8.Valid(b) }
