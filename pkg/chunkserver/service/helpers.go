package service

import (
	"crypto/md5"
	"io"
	"os"
)

// computeChecksum computes MD5 checksum of a file
func (cs *ChunkServerService) computeChecksum(filePath string) []byte {
	file, err := os.Open(filePath)
	if err != nil {
		return nil
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return nil
	}

	return hash.Sum(nil)
}
