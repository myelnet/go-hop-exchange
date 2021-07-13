package filetype

import (
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
// If no extension is given, it will try to guess is by using the first 512 bytes of a file
func Detect(path string) Type {
	ext := Ext(path)
	if ext != "" {
		return match(ext)
	}

	mtype, err := mimetype.DetectFile(path)
	if err != nil {
		return Unknown
	}

	ext = Ext(mtype.Extension())

	return match(ext)
}

// Ext returns the file extension.
// filepath.Ext() does not handle correctly extensions like .tar.gz
func Ext(path string) string {
	filename := filepath.Base(path)
	splitFilename := strings.Split(filename, ".")
	if len(splitFilename) == 0 {
		return ""
	}

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
