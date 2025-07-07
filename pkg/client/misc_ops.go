package client

import (
    "fmt"
    "log"

    "github.com/hanwen/go-fuse/v2/fuse"
)

func (fs *Filesystem) Init(server *fuse.Server) {
    log.Printf("FUSE filesystem initialized successfully")
    log.Printf("Mount point: %s", fs.config.MountPoint)
    log.Printf("Debug mode: %v", fs.config.Debug)
}

func (fs *Filesystem) String() string {
    return fmt.Sprintf("DFS Filesystem [mount=%s, debug=%v, server=%s]", 
        fs.config.MountPoint, 
        fs.config.Debug,
        fs.config.MasterAddr)
}

func (fs *Filesystem) SetDebug(debug bool) {
    fs.config.Debug = debug
    if debug {
        log.Printf("Debug mode enabled for filesystem")
    } else {
        log.Printf("Debug mode disabled for filesystem")
    }
}

// OnUnmount is called after processing the last request
func (fs *Filesystem) OnUnmount() {
    // Cleanup filesystem resources
    if err := fs.Close(); err != nil {
        log.Printf("Error during filesystem cleanup: %v", err)
    }
    
    log.Printf("FUSE filesystem unmounted successfully")
}

// Ioctl handles ioctl system calls
func (fs *Filesystem) Ioctl(cancel <-chan struct{}, input *fuse.IoctlIn, inbuf []byte, output *fuse.IoctlOut, outbuf []byte) (code fuse.Status) {
	return fuse.ENOTSUP
}