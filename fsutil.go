package cas

import (
	"errors"
	"os"
)

var errFileNotExist = errors.New("file does not exist")

func openFile(path string) (*os.File, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errFileNotExist
		}
		return nil, err
	}
	return f, nil
}
