package filetype

import (
	"path/filepath"
	"strings"

	"github.com/gabriel-vasile/mimetype"
)

type Type int

const (
	Application Type = iota
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

func Detect(path string, types ...Type) bool {
	ext := Ext(path)
	if ext != "" {
		return match(ext, types...)
	}

	mtype, err := mimetype.DetectFile(path)
	if err != nil {
		return false
	}

	ext = Ext(mtype.Extension())

	return match(ext, types...)
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

func match(ext string, types ...Type) bool {
	for _, type_ := range types {
		switch type_ {
		case Application:
			_, ok := application[ext]
			if ok {
				return true
			}
		case Audio:
			_, ok := audio[ext]
			if ok {
				return true
			}
		case Video:
			_, ok := video[ext]
			if ok {
				return true
			}
		case Image:
			_, ok := image[ext]
			if ok {
				return true
			}
		case Chemical:
			_, ok := chemical[ext]
			if ok {
				return true
			}
		case Font:
			_, ok := font[ext]
			if ok {
				return true
			}
		case Message:
			_, ok := message[ext]
			if ok {
				return true
			}
		case Model:
			_, ok := model[ext]
			if ok {
				return true
			}
		case Text:
			_, ok := text[ext]
			if ok {
				return true
			}
		case Archive:
			_, ok := archive[ext]
			if ok {
				return true
			}
		}
	}

	return false
}
