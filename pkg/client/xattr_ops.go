package client

import (
	"context"
	"fmt"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/uttam-li/dfs/api/generated/master"
)

const (
	// Extended attribute operation flags
	XATTR_CREATE  = 0x1 // Create attribute, fail if exists
	XATTR_REPLACE = 0x2 // Replace attribute, fail if not exists

	// RPC timeout for extended attribute operations
	xattrTimeout = 3 * time.Second
)

// GetXAttr retrieves the value of an extended attribute for the specified inode.
// If dest is nil, returns only the size of the attribute value.
func (fs *Filesystem) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (sz uint32, code fuse.Status) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), xattrTimeout)
	defer cancelCtx()

	resp, err := fs.masterClient.GetXAttr(ctx, &master.GetXAttrRequest{
		Ino:  header.NodeId,
		Name: attr,
	})
	if err != nil {
		return 0, fs.convertXAttrError(err)
	}

	value := resp.Value

	if dest == nil {
		return uint32(len(value)), fuse.OK
	}

	if len(dest) < len(value) {
		return uint32(len(value)), fuse.ERANGE
	}

	copy(dest, value)
	return uint32(len(value)), fuse.OK
}

// ListXAttr returns a list of extended attribute names for the specified inode.
// If dest is nil, returns only the total size needed for all attribute names.
func (fs *Filesystem) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), xattrTimeout)
	defer cancelCtx()

	resp, err := fs.masterClient.ListXAttr(ctx, &master.ListXAttrRequest{
		Ino: header.NodeId,
	})
	if err != nil {
		return 0, grpcToFuseStatus(err)
	}

	totalSize := fs.calculateXAttrListSize(resp.Names)

	if dest == nil {
		return totalSize, fuse.OK
	}

	if uint32(len(dest)) < totalSize {
		return totalSize, fuse.ERANGE
	}

	fs.copyXAttrNamesToBuffer(resp.Names, dest)
	return totalSize, fuse.OK
}

// SetXAttr sets or updates an extended attribute for the specified inode.
func (fs *Filesystem) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	if attr == "" {
		return fuse.EINVAL
	}

	ctx, cancelCtx := context.WithTimeout(context.Background(), xattrTimeout)
	defer cancelCtx()

	_, err := fs.masterClient.SetXAttr(ctx, &master.SetXAttrRequest{
		Ino:   input.NodeId,
		Name:  attr,
		Value: data,
		Flags: input.Flags,
	})
	if err != nil {
		return grpcToFuseStatus(err)
	}

	fs.cache.Invalidate(input.NodeId)
	return fuse.OK
}

// RemoveXAttr removes an extended attribute from the specified inode.
func (fs *Filesystem) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) (code fuse.Status) {
	if attr == "" {
		return fuse.EINVAL
	}

	ctx, cancelCtx := context.WithTimeout(context.Background(), xattrTimeout)
	defer cancelCtx()

	_, err := fs.masterClient.RemoveXAttr(ctx, &master.RemoveXAttrRequest{
		Ino:  header.NodeId,
		Name: attr,
	})
	if err != nil {
		return fs.convertXAttrError(err)
	}

	fs.cache.Invalidate(header.NodeId)
	return fuse.OK
}

// Helper functions

// convertXAttrError converts gRPC errors to appropriate FUSE status codes
// for extended attribute operations, ensuring ENODATA is returned for
// missing attributes instead of ENOENT.
func (fs *Filesystem) convertXAttrError(err error) fuse.Status {
	status := grpcToFuseStatus(err)
	if status == fuse.ENOENT {
		errStr := fmt.Sprintf("%v", err)
		if errStr == "attribute not found" {
			return fuse.Status(syscall.ENODATA)
		}
	}
	return status
}

// calculateXAttrListSize calculates the total buffer size needed for
// all extended attribute names including null terminators.
func (fs *Filesystem) calculateXAttrListSize(names []string) uint32 {
	var totalSize uint32
	for _, name := range names {
		totalSize += uint32(len(name) + 1) // +1 for null terminator
	}
	return totalSize
}

// copyXAttrNamesToBuffer copies extended attribute names to the destination
// buffer with null terminators between each name.
func (fs *Filesystem) copyXAttrNamesToBuffer(names []string, dest []byte) {
	offset := 0
	for _, name := range names {
		copy(dest[offset:], name)
		offset += len(name)
		dest[offset] = 0 // null terminator
		offset++
	}
}
