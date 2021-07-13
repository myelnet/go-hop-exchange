package filetype

import (
	"io"
	"path/filepath"
	"strings"

	"github.com/gabriel-vasile/mimetype"
)

type Type int

const (
	Unknown Type = iota
	Application
	Audio
	Video
	Image
	Chemical
	Font
	Message
	Model
	Text
	Archive
)

// Detect detects the Type of a file using it's extension.
// If no extension is given, it will try to guess is by using the first 512 bytes of a file.
// The path argument should be absolute
func Detect(path string, buf io.ReadSeeker) Type {
	ext := Ext(path)
	if ext != "" {
		return match(ext)
	}

	mtype, err := mimetype.DetectFile(path)
	if err != nil {
		// we need to rewind the buf since the mimetype lib will consume
		// the first bytes to detect the file type
		defer buf.Seek(0, io.SeekStart)

		mtype, err = mimetype.DetectReader(buf)
		if err != nil || mtype.Extension() == "" {
			return Unknown
		}
	}

	ext = Ext(mtype.Extension())

	return match(ext)
}

// Ext returns the file's extension.
// filepath.Ext() does not handle correctly extensions like .tar.gz
func Ext(path string) string {
	filename := filepath.Base(path)
	splitFilename := strings.Split(filename, ".")

	return strings.Join(splitFilename[1:], ".")
}

// match matches an extension with its Type, i.e : match("file.zip") returns Archive
func match(ext string) Type {
	mime, ok := AllMimes[ext]
	if ok {
		return mime.type_
	}

	return Unknown
}
